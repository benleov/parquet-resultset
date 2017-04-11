import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import parquet.resultset.ResultSetTransformer;
import parquet.resultset.TransformerListener;
import parquet.resultset.impl.ResultSetGenericTransformer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;

// http://www.programcreek.com/java-api-examples/index.php?class=org.apache.avro.file.DataFileReader&method=next

/**
 * Note, to run this on windows, you will need to to
 *
 * 1. Install Hadoop for windows
 *    Download from: https://github.com/karthikj1/Hadoop-2.7.1-Windows-64-binaries and unzip.
 *    Associated error message: "java.io.IOException: (null) entry in command string: null chmod"
 *
 * 2. Ensure that Microsoft Visual C++ 2010 SP1 Redistributable Package (x64) is installed.
 *    Download from: https://www.microsoft.com/en-us/download/confirmation.aspx?id=13523
 *    Associated error message: https://issues.apache.org/jira/browse/YARN-3524
 *
 * 3. Set the environment variable HADOOP_HOME to the location Hadoop was installed.
 *
 */
public class ResultSetParquetTransformerTest {

    @Test
    public void testToParquet() throws IOException, SQLException {

        String schemaName = "SchemaName";
        String namespace = "org.namespace";

        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);

        when(resultSet.next()).thenReturn(true, false); // return true for first next call
        when(resultSet.getInt("id")).thenReturn(99);

        when(resultSet.getMetaData()).thenReturn(metaData);

        // mock metadata so schema is created
        when(metaData.getColumnCount()).thenReturn(1);
        when(metaData.getColumnName(1)).thenReturn("id");
        when(metaData.getColumnType(1)).thenReturn(Types.INTEGER);

        List<TransformerListener> listeners = new ArrayList<>();

        ResultSetTransformer transformer = new ResultSetGenericTransformer();
        InputStream inputStream = transformer.toParquet(resultSet, schemaName, namespace, listeners);

        File testOutput = new File("test.par");

        IOUtils.copy(inputStream, new FileOutputStream(testOutput));

        validate(testOutput);

    }

    public void validate(File file) throws IOException {
        // read the events back using GenericRecord

        DatumReader<GenericRecord> reader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> fileReader = new DataFileReader<>(file, reader);

        GenericRecord record = new GenericData.Record(fileReader.getSchema());

        System.out.println("Has records : " + fileReader.hasNext());

        while (fileReader.hasNext()) {
            fileReader.next(record);
            String bodyStr = record.get("id").toString();
            System.out.println("HERE: " + bodyStr);
        }

        fileReader.close();
     }

}
