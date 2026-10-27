import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import parquet.resultset.ResultSetTransformer;
import parquet.resultset.impl.ResultSetArvoTransformer;
import parquet.resultset.impl.ResultSetAvroTransformer;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

// http://www.programcreek.com/java-api-examples/index.php?class=org.apache.avro.file.DataFileReader&method=next

/**
 * Tests for {@link ResultSetAvroTransformer}.
 */
public class ResultSetAvroTransformerTest extends ResultSetTest {

    @Override
    protected ResultSetTransformer createTransformer() {
        return new ResultSetAvroTransformer();
    }

    @Test
    public void testCompression() throws IOException, SQLException {

        ResultSetTransformer transformer = new ResultSetAvroTransformer(CodecFactory.deflateCodec(6));

        toRecords(transformer.transform(createTypedResultSet(), SCHEMA_NAME, NAMESPACE));

        try (DataFileReader<GenericRecord> fileReader =
                     new DataFileReader<>(new File(TEMP_FILE_NAME), new GenericDatumReader<>())) {
            assertEquals("deflate", fileReader.getMetaString("avro.codec"));
        }

        assertTypedRecords(readRecords(new File(TEMP_FILE_NAME)));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testDeprecatedArvoTransformer() throws IOException, SQLException {

        List<GenericRecord> records = toRecords(new ResultSetArvoTransformer().transform(createTypedResultSet(),
                SCHEMA_NAME, NAMESPACE));

        assertTypedRecords(records);
    }

    /**
     * Reads all records from the produced avro file.
     *
     * @param file The file to read.
     * @throws IOException
     */
    @Override
    public List<GenericRecord> readRecords(File file) throws IOException {

        List<GenericRecord> records = new ArrayList<>();

        GenericDatumReader<GenericRecord> datumReader =
                new GenericDatumReader<>(null, null, ResultSetTransformer.dataModel());

        try (DataFileReader<GenericRecord> fileReader = new DataFileReader<>(file, datumReader)) {
            fileReader.forEach(records::add);
        }

        return records;
    }

}
