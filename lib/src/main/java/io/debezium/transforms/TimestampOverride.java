package io.debezium.transforms;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.data.Envelope.Operation;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Timestamp;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;

import static io.debezium.connector.AbstractSourceInfo.TIMESTAMP_KEY;
import static io.debezium.data.Envelope.FieldName.AFTER;
import static io.debezium.data.Envelope.FieldName.OPERATION;
import static io.debezium.data.Envelope.FieldName.SOURCE;
import static io.debezium.data.Envelope.Operation.CREATE;
import static io.debezium.data.Envelope.Operation.READ;
import static io.debezium.data.Envelope.Operation.UPDATE;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;
import static org.apache.kafka.common.config.ConfigDef.Width.LONG;
import static org.apache.kafka.common.config.ConfigDef.Width.SHORT;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * TODO
 *
 * @author Nicolas Estrada.
 */
@SuppressWarnings("InstanceVariableMayNotBeInitialized")
public class TimestampOverride<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimestampOverride.class);

    private static final Field CREATE_TS_FIELD = Field.create("create.ts.field")
      .withDisplayName("Create timestamp field")
      .withType(STRING)
      .withWidth(LONG)
      .withImportance(MEDIUM)
      .withDescription("The column which determines the create time and initial snapshot (ie. read) time.");

    private static final Field UPDATE_TS_FIELD = Field.create("update.ts.field")
      .withDisplayName("Update timestamp field")
      .withType(STRING)
      .withWidth(LONG)
      .withImportance(MEDIUM)
      .withDescription("The column which determines the update time.");

    private static final Field FALLBACK_TO_COMMIT_TIME = Field.create("fallback.to.commit.time")
      .withDisplayName("Fallback to commit time (deletes, truncates etc.)")
      .withType(BOOLEAN)
      .withWidth(SHORT)
      .withImportance(LOW)
      .withDefault(false)
      .withValidation(Field::isBoolean)
      .withDescription("Whether delete and truncate operations (or other operations where the field is absent) " +
        "should have their timestamps overridden to the 'source.ts_ms' commit time.");

    private SmtManager<R> smtManager;
    private String createFieldName;
    private String updateFieldName;
    private boolean fallbackToCommitTime;

    @Override
    public void configure(final Map<String, ?> configs) {
        final var config = Configuration.from(configs);
        smtManager = new SmtManager<>(config);

        final var configFields = Field.setOf(FALLBACK_TO_COMMIT_TIME);
        if (!config.validateAndRecord(configFields, LOGGER::error)) {
            throw new ConnectException("Unable to validate config.");
        }
        createFieldName = config.getString(CREATE_TS_FIELD);
        updateFieldName = config.getString(UPDATE_TS_FIELD);
        fallbackToCommitTime = config.getBoolean(FALLBACK_TO_COMMIT_TIME);
    }

    @Override
    public R apply(final R record) {
        if (!smtManager.isValidEnvelope(record)) {
            return record;
        }
        final var value = requireStruct(record.value(), "Detect Debezium Operation");
        final var op = Operation.forCode(value.getString(OPERATION));
        final var fieldName = opFieldName(op);

        final Long timestamp = getEventTimestampMs(fieldName, value);
        return timestamp != null ? withTimestamp(record, timestamp) : record;
    }

    private R withTimestamp(final R record, final Long timestamp) {
        return record.newRecord(
          record.topic(), record.kafkaPartition(),
          record.keySchema(), record.key(),
          record.valueSchema(), record.value(),
          timestamp,
          record.headers());
    }

    private String opFieldName(final Operation op) {
        String fieldName = null;
        if (op == READ || op == CREATE) {
            fieldName = createFieldName;
        } else if (op == UPDATE) {
            fieldName = updateFieldName;
        }
        return fieldName;
    }

    @Override
    public ConfigDef config() {
        final var config = new ConfigDef();
        Field.group(config, null, CREATE_TS_FIELD, UPDATE_TS_FIELD, FALLBACK_TO_COMMIT_TIME);
        return config;
    }

    @Override
    public void close() {
    }

    /**
     * @return the Kafka record timestamp for the outgoing record.
     */
    private Long getEventTimestampMs(final String fieldName, final Struct envelopeValue) {
        final var after = envelopeValue.getStruct(AFTER);
        final var defaultValue = fallbackToCommitTime ? envelopeValue.getStruct(SOURCE).getInt64(TIMESTAMP_KEY) : null;

        final Object field;
        //noinspection NestedAssignment
        if (fieldName == null || (field = after.get(fieldName)) == null) {
            return defaultValue;
        }

        final long timestamp;
        final var schemaName = schemaNameForTsField(fieldName, after);
        switch (schemaName) {
            case org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME:
                //noinspection UseOfObsoleteDateTimeApi
                timestamp = ((Date) field).getTime();
                break;
            case Timestamp.SCHEMA_NAME:
                timestamp = (long) field;
                break;
            case MicroTimestamp.SCHEMA_NAME:
                timestamp = MICROSECONDS.toMillis((Long) field);
                break;
            case NanoTimestamp.SCHEMA_NAME:
                timestamp = NANOSECONDS.toMillis((Long) field);
                break;
            default:
                throw new ConnectException(format("Unsupported field type %s for event timestamp", schemaName));
        }
        return timestamp;
    }

    private static String schemaNameForTsField(final String fieldName, final Struct after) {
        final var tsField = after.schema().field(fieldName);
        final var schemaName = tsField.schema().name();
        if (schemaName == null) {
            throw new ConnectException(
              format("Unsupported field type %s (without logical schema name) for event timestamp",
                tsField.schema().type()));
        }
        return schemaName;
    }
}
