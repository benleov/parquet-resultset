package parquet.resultset.impl;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Transforms a ResultSet into an avro data file.
 */
public class ResultSetAvroTransformer extends AbstractResultSetTransformer {

    private final CodecFactory codec;

    /**
     * Writes without compression.
     */
    public ResultSetAvroTransformer() {
        this(CodecFactory.nullCodec());
    }

    /**
     * @param codec Compression codec, e.g. {@link CodecFactory#snappyCodec()} or
     *              {@link CodecFactory#deflateCodec(int)}.
     */
    public ResultSetAvroTransformer(CodecFactory codec) {
        this.codec = codec;
    }

    @Override
    protected RecordWriter createWriter(Schema schema, GenericData dataModel, OutputStream outputStream)
            throws IOException {

        DataFileWriter<GenericRecord> dataFileWriter =
                new DataFileWriter<>(new GenericDatumWriter<GenericRecord>(schema, dataModel));

        dataFileWriter.setCodec(codec);
        dataFileWriter.create(schema, outputStream);

        return new RecordWriter() {

            @Override
            public void write(GenericRecord record) throws IOException {
                dataFileWriter.append(record);
            }

            @Override
            public void close() throws IOException {
                dataFileWriter.close();
            }
        };
    }
}
