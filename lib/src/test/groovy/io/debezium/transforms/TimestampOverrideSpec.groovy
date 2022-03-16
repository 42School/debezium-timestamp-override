package io.debezium.transforms

import io.debezium.data.Envelope
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.Timestamp
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.source.SourceRecord
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Subject
import spock.lang.Unroll

import static java.lang.System.currentTimeMillis
import static java.time.Instant.now
import static java.time.temporal.ChronoUnit.DAYS
import static org.apache.kafka.connect.data.Schema.*
import static org.apache.kafka.connect.data.SchemaBuilder.struct

class TimestampOverrideSpec extends Specification {

    @Subject
    def xform = new TimestampOverride()

    def sourceSchema = struct()
      .field('lsn', INT32_SCHEMA)
      .field('ts_ms', INT64_SCHEMA)
      .build()

    @Shared
    def commitTs = now()
    def source = toStruct sourceSchema, [
      lsn  : 1234,
      ts_ms: commitTs.toEpochMilli()
    ]

    def createdAt = Date.from(now().minus(1, DAYS))
    def updatedAt = new Date(createdAt.time + 10000L)

    def recordSchema = struct()
      .field('id', INT32_SCHEMA)
      .field('name', STRING_SCHEMA)
      .field('created_at', Timestamp.SCHEMA)
      .field('updated_at', Timestamp.builder().optional().schema())
      .build()

    def initial = toStruct recordSchema, [
      id        : 1,
      name      : 'myRecord',
      created_at: createdAt,
      updated_at: createdAt
    ]

    def updated = toStruct recordSchema, [
      id        : 1,
      name      : 'myRecord2',
      created_at: createdAt,
      updated_at: updatedAt
    ]

    def envelope = Envelope.defineSchema()
      .withName('dummy.Envelope')
      .withRecord(recordSchema)
      .withSource(sourceSchema)
      .build()

    def 'All fields should have a description'() {

        expect:
        xform
          .config()
          .configKeys()
          .values()
          .every { it.documentation }
    }

    def 'Configuration must be valid'() {

        when:
        xform.configure([
          'fallback.to.commit.time': 'what'
        ])

        then:
        thrown(ConnectException)
    }

    def 'If not a valid envelope do nothing'() {

        given:
        xform.configure([
          'create.ts.field': 'created_at'
        ])

        when:
        def sr = new SourceRecord([:], [:], 'dummy', null, null)

        then:
        xform.apply(sr) === sr
    }

    @Unroll
    def 'The created timestamp should override the record timestamp on #op'() {

        given:
        xform.configure([
          'create.ts.field': 'created_at'
        ])

        when:
        def evt = dummyRecord envelope."$op"(initial, source, now())

        then:
        evt.timestamp() == null
        xform.apply(evt).timestamp() == initial.created_at.time

        where:
        op << ['read', 'create']

    }

    def 'The updated timestamp should override the record timestamp on update'() {

        given:
        xform.configure([
          'update.ts.field': 'updated_at'
        ])

        when:
        def evt = dummyRecord envelope.update(initial, updated, source, now())

        then:
        evt.timestamp() == null
        xform.apply(evt).timestamp() == updated.updated_at.time

    }

    def 'The commit time should override the record timestamp on delete'() {

        given:
        xform.configure([
          'fallback.to.commit.time': 'true'
        ])

        when:
        def evt = dummyRecord envelope.delete(initial, source, now())

        then:
        evt.timestamp() == null
        xform.apply(evt).timestamp() == source.ts_ms

    }

    def 'The commit time should override the record timestamp on truncate'() {

        given:
        xform.configure([
          'fallback.to.commit.time': 'true'
        ])

        when:
        def evt = dummyRecord envelope.truncate(source, now())

        then:
        evt.timestamp() == null
        xform.apply(evt).timestamp() == source.ts_ms

    }

    @Unroll
    def 'If the field "updated_at" is not present if should #behaviour'() {

        given:
        xform.configure([
          'fallback.to.commit.time': "$fallback"
        ])

        when:
        def after = updated.put 'updated_at', null
        def evt = dummyRecord envelope.update(initial, after, source, now())

        then:
        evt.timestamp() == null
        xform.apply(evt).timestamp() == expected

        where:
        fallback | expected                | behaviour
        true     | commitTs.toEpochMilli() | 'fallback to commit time'
        false    | null                    | 'leave timestamp unchanged'

    }

    def 'If the field is not defined it should fail fast'() {

        given:
        xform.configure(['create.ts.field': 'created_ts'])

        when:
        def evt = dummyRecord envelope.read(initial, source, now())

        and:
        xform.apply evt

        then:
        thrown(DataException)

    }

    def dummyRecord(payload) {
        new SourceRecord([:], [:], 'dummy', envelope.schema(), payload)
    }

    void cleanup() {
        xform.close()
    }

    private toStruct(schema, Map m) {
        def struct = new Struct(schema)
        m.each { k, v ->
            if (v != null) {
                struct.put(k as String, v)
            }
        }
        struct
    }

    static {
        Struct.metaClass.getAt = { key ->
            delegate.get key
        }
    }
}
