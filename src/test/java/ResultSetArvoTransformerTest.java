import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import parquet.resultset.impl.ResultSetArvoTransformer;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

// http://www.programcreek.com/java-api-examples/index.php?class=org.apache.avro.file.DataFileReader&method=next

/**
 * Tests for {@link ResultSetArvoTransformer}.
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

    @Test
    public void testTypesAndNulls() throws IOException, SQLException {
        super.testTypesAndNulls(new ResultSetArvoTransformer());
    }

    /**
     * Reads all records from the produced arvo file.
     *
     * @param file The file to read.
     * @throws IOException
     */
    @Override
    public List<GenericRecord> readRecords(File file) throws IOException {

        List<GenericRecord> records = new ArrayList<>();

        try (DataFileReader<GenericRecord> fileReader = new DataFileReader<>(file, new GenericDatumReader<>())) {
            fileReader.forEach(records::add);
        }

        return records;
    }

}
