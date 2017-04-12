# parquet-resultset

The parquet-resultset library can be used to convert standard sql result sets 
into [parquet](https://parquet.apache.org/). 

For example

``` java

        /* Standard SQL connection stuff here, where statement is a standard java.sql.Statement. */

        ResultSet resultSet = statement.executeQuery("SELECT * FROM Widget");

        String mySchemaName = "Widget";
        String namespace = "org.mycompany.widgets";
        
        // here listeners can be added which will be notified when records are parsed
        List<TransformerListener> listeners = new ArrayList<>();

        ResultSetTransformer transformer = new ResultSetGenericTransformer();
        InputStream inputStream = transformer.toParquet(resultSet, schemaName, namespace, listeners); 
        
```

The input stream can the be written out to file, or, for example, to S3, which can then be queried by 
[Athena](https://aws.amazon.com/athena/).