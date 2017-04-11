package parquet.resultset.impl;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import parquet.resultset.*;

import java.io.*;
import java.nio.file.Files;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class ResultSetGenericTransformer implements ResultSetTransformer {

    public InputStream toParquet(ResultSet resultSet, String schemaName, String namespace,
                                  List<TransformerListener> listeners) throws IOException, SQLException {

        SchemaResults schemaResults = new ResultSetSchemaGenerator().generateSchema(resultSet,
                schemaName, namespace);

        listeners.forEach(transformerListener -> transformerListener.onSchemaParsed(schemaResults));

        DatumWriter<GenericRecord> dw = new GenericDatumWriter<>(schemaResults.getParsedSchema());

        DataFileWriter<GenericRecord> dfw = new DataFileWriter<>(dw);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        dfw.create(schemaResults.getParsedSchema(), baos);


        while (resultSet.next()) {

            GenericRecordBuilder builder = new GenericRecordBuilder(schemaResults.getParsedSchema());

            for (SchemaSqlMapping mapping : schemaResults.getMappings()) {

                builder.set(
                        schemaResults.getParsedSchema().getField(mapping.getSchemaName()),
                        ResultSetTransformer.extractResult(mapping, resultSet));
            }

            dfw.append(builder.build());

            listeners.forEach(transformerListener -> transformerListener.onRecordParsed(resultSet));
        }


        dfw.flush();
        dfw.close();

        return new ByteArrayInputStream(baos.toByteArray());

    }


}
