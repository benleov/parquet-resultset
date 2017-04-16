import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.junit.Test;
import parquet.resultset.impl.ResultSetArvoTransformer;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

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
public class ResultSetArvoTransformerTest extends ResultSetTest {


    @Test
    public void testArvoListeners() throws IOException, SQLException {
        super.testListeners(new ResultSetArvoTransformer());
    }


    @Test
    public void testToFile() throws IOException, SQLException {
        super.testToResultFile(new ResultSetArvoTransformer());
    }

    /**
     * Validates produced arvo file.
     *
     * @param file The file to validate.
     * @throws IOException
     */
    @Override
    public void validate(File file, String fieldName, Integer... expectedValues) throws IOException {

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
