import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.Test;
import parquet.resultset.impl.ResultSetParquetTransformer;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

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

    @Test
    public void testTypesAndNulls() throws IOException, SQLException {
        super.testTypesAndNulls(new ResultSetParquetTransformer());
    }

    @Override
    public List<GenericRecord> readRecords(File file) throws IOException {

        List<GenericRecord> records = new ArrayList<>();

        try (ParquetReader<GenericRecord> reader =
                     AvroParquetReader.<GenericRecord>builder(new LocalInputFile(file.toPath())).build()) {

            for (GenericRecord record = reader.read(); record != null; record = reader.read()) {
                records.add(record);
            }
        }

        return records;
    }
}
