# debezium-timestamp-override
SMT for Kafka Connect to override event timestamps from Debezium

## Motivation
After working with Debezium and [Kafka Streams]/[ksqlDB] for a while, [joining streams] proved difficult for historical
data from [Debezium] into new streams due to the fact that initial snapshots were always using wall-clock time.

Since [ksqlDB] does not  yet support Global KTables and timestamp extraction proved difficult with tombstone events and had to be applied
everywhere.

This SMT provides the means, if the underlying database provides `created_at` and/or `updated_at` columns, to be able
to override the record timestamp at the source, making stream-table joins a lot easier.

### Configuration

#### `create.ts.field`

The column which determines the creation time and initial snapshot (i.e. read) time.

- *Importance:* MEDIUM
- *Type:* STRING

#### `update.ts.field`

The column which determines the update time.

- *Importance:* MEDIUM
- *Type:* STRING

#### `fallback.to.commit.time`

Whether delete and truncate operations (or other operations where the field is absent) should have their timestamps
overridden to the `source.ts_ms` commit time.

- *Importance:* LOW
- *Type:* BOOLEAN
- *Default Value:* false

### Usage example

```
"transforms": "tsOverride",
"transforms.tsOverride.type": "io.debezium.transforms.TimestampOverride",
"transforms.tsOverride.create.ts.field": "created_at",
"transforms.tsOverride.update.ts.field": "updated_at",
"transforms.tsOverride.fallback.to.commit.time": "true",
```

## Development

### Building the source

```bash
./gradlew clean build
```

## Contributions

Contributions are welcome! Especially those which can help me package it correctly and upload to [Confluent Hub].

[Kafka Streams]: https://kafka.apache.org/documentation/streams/
[ksqlDB]: https://ksqldb.io/
[joining streams]: https://www.confluent.io/events/kafka-summit-europe-2021/temporal-joins-in-kafka-streams-and-ksqldb/
[Debezium]: https://debezium.io/
[Confluent Hub]: https://www.confluent.io/hub/
