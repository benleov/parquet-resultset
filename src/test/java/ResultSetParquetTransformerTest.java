import org.junit.Test;
import parquet.resultset.impl.ResultSetArvoTransformer;
import parquet.resultset.impl.ResultSetParquetTransformer;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

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

        // TODO: read parquet file and validate

    }
}
