package parquet.resultset.impl;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import parquet.resultset.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 *
 */
public class ResultSetParquetTransformer implements ResultSetTransformer {

    public InputStream transform(ResultSet resultSet, String schemaName, String namespace,
                                 List<TransformerListener> listeners) throws IOException, SQLException {

        SchemaResults schemaResults = new ResultSetSchemaGenerator().generateSchema(resultSet,
                schemaName, namespace);

        listeners.forEach(transformerListener -> transformerListener.onSchemaParsed(schemaResults));

        // written through the local file system directly, so hadoop's winutils is not needed on windows
        Path tempFile = Files.createTempFile(null, ".parquet");

        try (ParquetWriter<GenericRecord> parquetWriter = AvroParquetWriter
                .<GenericRecord>builder(new LocalOutputFile(tempFile))
                .withSchema(schemaResults.getParsedSchema())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build()) {

            while (resultSet.next()) {

                GenericRecordBuilder builder = new GenericRecordBuilder(schemaResults.getParsedSchema());

                for (SchemaSqlMapping mapping : schemaResults.getMappings()) {

                    builder.set(
                            schemaResults.getParsedSchema().getField(mapping.getSchemaName()),
                            ResultSetTransformer.extractResult(mapping, resultSet));
                }

                GenericRecord record = builder.build();

                listeners.forEach(transformerListener -> transformerListener.onRecordParsed(record));

                parquetWriter.write(record);
            }
        } catch (IOException | SQLException | RuntimeException e) {
            Files.deleteIfExists(tempFile);
            throw e;
        }

        // the temp file is removed once the caller closes the stream
        return Files.newInputStream(tempFile, StandardOpenOption.DELETE_ON_CLOSE);
    }
}
