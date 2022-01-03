# parquet-resultset

The parquet-resultset library can be used to convert a standard SQL 
[ResultSet](https://docs.oracle.com/javase/8/docs/api/java/sql/ResultSet.html) into 
[Parquet](https://parquet.apache.org/), or  [Arvo](https://avro.apache.org/)

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
        // or new ResultSetArvoTransformer()
        InputStream inputStream = transformer.toParquet(resultSet, schemaName, namespace, listeners); 
        
```

The input stream can be written out to anywhere, i.e a file or S3, which can then be queried by 
[Athena](https://aws.amazon.com/athena/).

## Building

```
./gradlew build
```

## License

This project is Open Source software released under the https://www.apache.org/licenses/LICENSE-2.0.html[Apache 2.0 license].
