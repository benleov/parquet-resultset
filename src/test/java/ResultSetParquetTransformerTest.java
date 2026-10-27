import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;
import parquet.resultset.ResultSetTransformer;
import parquet.resultset.impl.ResultSetParquetTransformer;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Tests for {@link ResultSetParquetTransformer}.
 */
public class ResultSetParquetTransformerTest extends ResultSetTest {

    @Override
    protected ResultSetTransformer createTransformer() {
        return new ResultSetParquetTransformer();
    }

    @Test
    public void testCompression() throws IOException, SQLException {

        ResultSetTransformer transformer = new ResultSetParquetTransformer(CompressionCodecName.GZIP);

        toRecords(transformer.transform(createTypedResultSet(), SCHEMA_NAME, NAMESPACE));

        try (ParquetFileReader fileReader = ParquetFileReader.open(new LocalInputFile(new File(TEMP_FILE_NAME).toPath()))) {

            List<BlockMetaData> blocks = fileReader.getFooter().getBlocks();
            assertFalse(blocks.isEmpty());

            for (ColumnChunkMetaData column : blocks.get(0).getColumns()) {
                assertEquals(CompressionCodecName.GZIP, column.getCodec(), column.getPath().toDotString());
            }
        }

        assertTypedRecords(readRecords(new File(TEMP_FILE_NAME)));
    }

    /**
     * Checks the parquet column types that query engines such as Athena use to read the file.
     */
    @Test
    public void testParquetLogicalTypes() throws IOException, SQLException {

        toRecords(createTransformer().transform(createTypedResultSet(), SCHEMA_NAME, NAMESPACE));

        MessageType schema;

        try (ParquetFileReader fileReader = ParquetFileReader.open(new LocalInputFile(new File(TEMP_FILE_NAME).toPath()))) {
            schema = fileReader.getFooter().getFileMetaData().getSchema();
        }

        assertEquals(LogicalTypeAnnotation.decimalType(5, 23), logicalType(schema, "decimal_col"));
        assertEquals(LogicalTypeAnnotation.decimalType(0, 30), logicalType(schema, "numeric30_col"));
        assertEquals(LogicalTypeAnnotation.dateType(), logicalType(schema, "date_col"));
        assertEquals(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS),
                logicalType(schema, "time_col"));
        assertEquals(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS),
                logicalType(schema, "timestamp_col"));
        assertEquals(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS),
                logicalType(schema, "timestamptz_col"));
        assertEquals(LogicalTypeAnnotation.stringType(), logicalType(schema, "varchar_col"));
        assertEquals(LogicalTypeAnnotation.decimalType(0, 20), logicalType(schema, "ubigint_col"));

        // arrays use the standard 3 level list: optional group (LIST) { repeated group list { optional element } }
        assertEquals(LogicalTypeAnnotation.listType(), logicalType(schema, "int_array_col"));
        GroupType list = schema.getType("int_array_col").asGroupType();
        GroupType repeated = list.getType(0).asGroupType();
        assertEquals("list", repeated.getName());
        assertEquals(Type.Repetition.REPEATED, repeated.getRepetition());
        assertEquals(Type.Repetition.OPTIONAL, repeated.getType("element").getRepetition());
        assertEquals(PrimitiveType.PrimitiveTypeName.INT32,
                repeated.getType("element").asPrimitiveType().getPrimitiveTypeName());
    }

    private static LogicalTypeAnnotation logicalType(MessageType schema, String fieldName) {
        return schema.getType(fieldName).getLogicalTypeAnnotation();
    }

    @Override
    public List<GenericRecord> readRecords(File file) throws IOException {

        List<GenericRecord> records = new ArrayList<>();

        try (ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord>builder(new LocalInputFile(file.toPath()))
                .withDataModel(ResultSetTransformer.dataModel())
                .build()) {

            for (GenericRecord record = reader.read(); record != null; record = reader.read()) {
                records.add(record);
            }
        }

        return records;
    }
}
