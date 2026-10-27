package parquet.resultset.impl;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Transforms a ResultSet into a parquet file.
 */
public class ResultSetParquetTransformer extends AbstractResultSetTransformer {

    private final CompressionCodecName compressionCodec;
    private final long rowGroupSize;

    /**
     * Uses snappy compression and parquet's default row group size.
     */
    public ResultSetParquetTransformer() {
        this(CompressionCodecName.SNAPPY);
    }

    /**
     * @param compressionCodec Compression codec, e.g. SNAPPY, GZIP, ZSTD or UNCOMPRESSED.
     */
    public ResultSetParquetTransformer(CompressionCodecName compressionCodec) {
        this(compressionCodec, ParquetWriter.DEFAULT_BLOCK_SIZE);
    }

    /**
     * @param compressionCodec Compression codec, e.g. SNAPPY, GZIP, ZSTD or UNCOMPRESSED.
     * @param rowGroupSize     Row group size in bytes. Larger groups compress better but use more memory while
     *                         writing, as each group is buffered until it is full.
     */
    public ResultSetParquetTransformer(CompressionCodecName compressionCodec, long rowGroupSize) {
        this.compressionCodec = compressionCodec;
        this.rowGroupSize = rowGroupSize;
    }

    @Override
    protected RecordWriter createWriter(Schema schema, GenericData dataModel, OutputStream outputStream)
            throws IOException {

        // written through an OutputFile rather than the hadoop file system, so winutils is not needed on windows
        ParquetWriter<GenericRecord> parquetWriter = AvroParquetWriter
                .<GenericRecord>builder(new StreamOutputFile(outputStream))
                .withSchema(schema)
                .withDataModel(dataModel)
                .withCompressionCodec(compressionCodec)
                .withRowGroupSize(rowGroupSize)
                // the standard 3 level list structure, which allows null array elements
                .config(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, "false")
                .build();

        return new RecordWriter() {

            @Override
            public void write(GenericRecord record) throws IOException {
                parquetWriter.write(record);
            }

            @Override
            public void close() throws IOException {
                parquetWriter.close();
            }
        };
    }
}
