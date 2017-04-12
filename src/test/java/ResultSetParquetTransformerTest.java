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

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// http://www.programcreek.com/java-api-examples/index.php?class=org.apache.avro.file.DataFileReader&method=next

/**
 * Note, to run this on windows, you will need to to
 * <p>
 * 1. Install Hadoop for windows
 * Download from: https://github.com/karthikj1/Hadoop-2.7.1-Windows-64-binaries and unzip.
 * Associated error message: "java.io.IOException: (null) entry in command string: null chmod"
 * <p>
 * 2. Ensure that Microsoft Visual C++ 2010 SP1 Redistributable Package (x64) is installed.
 * Download from: https://www.microsoft.com/en-us/download/confirmation.aspx?id=13523
 * Associated error message: https://issues.apache.org/jira/browse/YARN-3524
 * <p>
 * 3. Set the environment variable HADOOP_HOME to the location Hadoop was installed.
 * <p>
 * <p>
 * http://www.programcreek.com/java-api-examples/index.php?source_dir=mypipe-master/avro/lang/java/trevni/avro/src/test/java/org/apache/trevni/avro/TestEvolvedSchema.java
 * http://www.programcreek.com/java-api-examples/index.php?class=org.apache.avro.file.DataFileReader&method=next
 */
public class ResultSetParquetTransformerTest {

    private static final String ID_FIELD_NAME = "id";
    private static final String TEMP_FILE_NAME = "unit_test.tmp";

    @Test
    public void testToParquet() throws IOException, SQLException {

        String schemaName = "SchemaName";
        String namespace = "org.namespace";

        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);

        when(resultSet.next()).thenReturn(true, true, true, true, false); // return true for first next call
        when(resultSet.getInt(ID_FIELD_NAME)).thenReturn(0, 1, 2, 3, 4 );

        when(resultSet.getMetaData()).thenReturn(metaData);

        // mock metadata so schema is created
        when(metaData.getColumnCount()).thenReturn(1);
        when(metaData.getColumnName(1)).thenReturn(ID_FIELD_NAME);
        when(metaData.getColumnType(1)).thenReturn(Types.INTEGER);

        List<TransformerListener> listeners = new ArrayList<>();

        ResultSetTransformer transformer = new ResultSetGenericTransformer();
        InputStream inputStream = transformer.toParquet(resultSet, schemaName, namespace, listeners);

        File testOutput = new File(TEMP_FILE_NAME);

        try {
            IOUtils.copy(inputStream, new FileOutputStream(testOutput));
            validate(testOutput);
        } finally {
            testOutput.delete();
        }
    }

    /**
     * Validates produced parquet file.
     *
     * @param file The file to validate.
     *
     * @throws IOException
     */
    private void validate(File file) throws IOException {

        DatumReader<GenericRecord> reader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> fileReader = new DataFileReader<>(file, reader);

        GenericRecord record = new GenericData.Record(fileReader.getSchema());

        int x = 0;
        boolean recordsRead = false;

        while (fileReader.hasNext()) {
            recordsRead = true;

            fileReader.next(record);
            assertEquals(x++, record.get(ID_FIELD_NAME));
        }

        assertTrue("No records were read from file.", recordsRead);

        fileReader.close();
    }

}
