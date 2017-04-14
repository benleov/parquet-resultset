import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import parquet.resultset.ResultSetTransformer;
import parquet.resultset.SchemaResults;
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
import java.util.Arrays;
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
    private static final String SCHEMA_NAME = "SchemaName";
    private static final String NAMESPACE = "org.NAMESPACE";
    private static final Integer[] ID_VALUES = {0, 1, 2, 3, 4, 5, 6};

    private ResultSet resultSet = mock(ResultSet.class);
    private ResultSetMetaData metaData = mock(ResultSetMetaData.class);


    @Before
    public void before() throws Exception {

        Boolean[] nextReturns = new Boolean[ID_VALUES.length + 1]; 
        Arrays.fill(nextReturns, Boolean.TRUE);
        nextReturns[nextReturns.length - 1] = false; // set last value to false

        when(resultSet.next()).thenReturn(nextReturns[0], Arrays.copyOfRange(nextReturns, 1, nextReturns.length));
        when(resultSet.getInt(ID_FIELD_NAME)).thenReturn(ID_VALUES[0], Arrays.copyOfRange(ID_VALUES, 1, ID_VALUES.length));
        when(resultSet.getMetaData()).thenReturn(metaData);

        // mock metadata so schema is created
        when(metaData.getColumnCount()).thenReturn(1);
        when(metaData.getColumnName(1)).thenReturn(ID_FIELD_NAME);
        when(metaData.getColumnType(1)).thenReturn(Types.INTEGER);
    }

    @After
    public void after() {
        File testOutput = new File(TEMP_FILE_NAME);
        testOutput.delete();
    }

    @Test
    public void testToParquetResultFile() throws IOException, SQLException {

        List<TransformerListener> listeners = new ArrayList<>();

        ResultSetTransformer transformer = new ResultSetGenericTransformer();
        InputStream inputStream = transformer.toParquet(resultSet, SCHEMA_NAME, NAMESPACE, listeners);

        File testOutput = new File(TEMP_FILE_NAME);

        IOUtils.copy(inputStream, new FileOutputStream(testOutput));
        validate(testOutput, ID_FIELD_NAME, ID_VALUES);
    }

    @Test
    public void testToParquetListeners() throws IOException, SQLException {

        List<TransformerListener> listeners = new ArrayList<>();

        listeners.add(new TransformerListener() {
            @Override
            public void onRecordParsed(GenericRecord record) {
                System.out.println("RECORD PARSED " + record.get(ID_FIELD_NAME));
            }

            @Override
            public void onSchemaParsed(SchemaResults schemaResults) {

            }
        });

        ResultSetTransformer transformer = new ResultSetGenericTransformer();
        InputStream inputStream = transformer.toParquet(resultSet, SCHEMA_NAME, NAMESPACE, listeners);

    }


    /**
     * Validates produced parquet file.
     *
     * @param file The file to validate.
     * @throws IOException
     */
    private void validate(File file, String fieldName, Integer... expectedValues) throws IOException {

        DatumReader<GenericRecord> reader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> fileReader = new DataFileReader<>(file, reader);

        GenericRecord record = new GenericData.Record(fileReader.getSchema());

        int x = 0;
        boolean recordsRead = false;

        while (fileReader.hasNext()) {
            recordsRead = true;

            fileReader.next(record);
            assertEquals(expectedValues[x++], record.get(fieldName));
        }

        assertTrue("No records were read from file.", recordsRead);

        fileReader.close();
    }

}
