# parquet-resultset

The parquet-resultset library can be used to convert a standard SQL 
[ResultSet](https://docs.oracle.com/en/java/javase/11/docs/api/java.sql/java/sql/ResultSet.html) into
[Parquet](https://parquet.apache.org/) or [Avro](https://avro.apache.org/).

For example

``` java

        /* Standard SQL connection stuff here, where statement is a standard java.sql.Statement. */

        ResultSet resultSet = statement.executeQuery("SELECT * FROM Widget");

        String mySchemaName = "Widget";
        String namespace = "org.mycompany.widgets";
        
        // here listeners can be added which will be notified when records are parsed
        List<TransformerListener> listeners = new ArrayList<>();

        listeners.add(new TransformerListener() {
        
            @Override
            public void onSchemaParsed(SchemaResults schemaResults) {
                // called when the schema is parsed
            }
            
            @Override
            public void onRecordParsed(GenericRecord record) {
                // called whenever a record/row is parsed
            }
        });

        ResultSetTransformer transformer = new ResultSetParquetTransformer();
        // or new ResultSetAvroTransformer()

        // write directly to a file, S3 upload stream etc. The stream is flushed but not closed.
        try (OutputStream outputStream = Files.newOutputStream(Paths.get("widget.parquet"))) {
            transformer.transform(resultSet, mySchemaName, namespace, listeners, outputStream);
        }
        
```

The output can be written out to anywhere, i.e a file or S3, which can then be queried by 
[Athena](https://aws.amazon.com/athena/). Only the current row (and for parquet, the current row group) is held
in memory. Some drivers read the whole result set into memory by default, e.g. Postgres needs auto commit off and
a fetch size (`statement.setFetchSize(1000)`), and MySQL needs `useCursorFetch=true` with a fetch size.

`transform` can also return an `InputStream` instead, without the listeners if they are not needed. It writes to
a temp file, which is deleted when the stream is closed:

``` java
        try (InputStream inputStream = transformer.transform(resultSet, mySchemaName, namespace)) {
            // read the transformed data
        }
```

## Options

``` java
        // compression codec (default SNAPPY) and row group size in bytes (default 128MB)
        new ResultSetParquetTransformer(CompressionCodecName.ZSTD, 64 * 1024 * 1024);

        // compression codec (default none)
        new ResultSetAvroTransformer(CodecFactory.snappyCodec());
```

## Type mapping

Every field is nullable. SQL NULLs are written as nulls, including for numeric and boolean columns.

| SQL type | Avro / Parquet type |
| --- | --- |
| BOOLEAN, BIT | boolean (BIT(n) with more than one bit: bytes) |
| TINYINT, SMALLINT, INTEGER | int |
| BIGINT | long |
| Unsigned INTEGER, e.g. MySQL `INT UNSIGNED` | long |
| Unsigned BIGINT | decimal(20, 0) |
| REAL | float |
| FLOAT, DOUBLE | double |
| NUMERIC, DECIMAL with scale 0 | int up to precision 9, long up to 18, then decimal |
| NUMERIC, DECIMAL with a scale | decimal with the column's precision and scale |
| NUMERIC, DECIMAL with unknown precision (e.g. Oracle `NUMBER`, Postgres `numeric`) or above 38 | double |
| DATE | date |
| TIME, TIME_WITH_TIMEZONE | time-millis |
| TIMESTAMP | local-timestamp-millis (no time zone) |
| TIMESTAMP_WITH_TIMEZONE | timestamp-millis (UTC) |
| BINARY, VARBINARY, LONGVARBINARY, BLOB | bytes |
| ARRAY | array (a Parquet LIST) of nullable elements, see below |
| Everything else, e.g. CHAR, VARCHAR, CLOB, SQLXML, OTHER (Postgres `uuid`, `json`) | string |

JDBC doesn't report the element type of an ARRAY column, so it's taken from the column's type name. The forms
used by Postgres (`_int4`), the SQL standard (`INTEGER ARRAY`, used by H2 and HSQLDB) and Hive/Spark
(`array<int>`) are recognised. Booleans, ints, longs, floats and doubles keep their type, decimals become
doubles, and everything else, including unrecognised type names, becomes strings (e.g. dates are written as
`2020-01-02`). Elements of multi-dimensional arrays are written as strings such as `[1, 2]` when the element
type is a string. For numeric types, e.g. a Postgres `int[][]` (which has the same type name as `int[]`), they
fail with an error.

Field names are the column labels (the `AS` alias), lower cased. Characters other than letters, digits and
underscores are replaced with underscores, e.g. `COUNT(*)` becomes `count___`, and repeated names (e.g. the `id`
columns of a join) get a suffix: `id`, `id_2`.

Records passed to listeners hold decimals, dates and times as `BigDecimal` and `java.time` values. To read them
back the same way, use `ResultSetTransformer.dataModel()`:

``` java
        new GenericDatumReader<GenericRecord>(null, null, ResultSetTransformer.dataModel());

        AvroParquetReader.<GenericRecord>builder(inputFile).withDataModel(ResultSetTransformer.dataModel());
```

## Building

Using the library requires Java 11 or later. Building it requires JDK 17 or later (for Gradle 9).

```
./gradlew build
```

## License

This project is Open Source software released under the
[Apache 2.0 license](https://www.apache.org/licenses/LICENSE-2.0.html).
