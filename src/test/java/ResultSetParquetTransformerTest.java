import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.Test;
import parquet.resultset.impl.ResultSetParquetTransformer;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class ResultSetParquetTransformerTest extends ResultSetTest {

    @Test
    public void testArvoListeners() throws IOException, SQLException {
        super.testListeners(new ResultSetParquetTransformer());
    }


    @Test
    public void testToFile() throws IOException, SQLException {
        super.testToResultFile(new ResultSetParquetTransformer());
    }

    @Override
    public void validate(File file, String fieldName, Integer... expectedValues) throws IOException {

        Path path = new Path(file.toString());
        ParquetReader<GenericRecord>  reader = AvroParquetReader.builder(path).build();

        int x = 0;
        boolean recordsRead = false;
        for(GenericRecord record = reader.read(); record != null; record = reader.read()) {

            recordsRead = true;
            assertEquals(expectedValues[x++], record.get(fieldName));
        }


        assertTrue(recordsRead);

        reader.close();
    }
}
