package parquet.resultset.impl;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;
import parquet.resultset.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 *
 */
public class ResultSetArvoTransformer implements ResultSetTransformer {

    public InputStream transform(ResultSet resultSet, String schemaName, String namespace,
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

            GenericRecord record = builder.build();

            listeners.forEach(transformerListener -> transformerListener.onRecordParsed(record));

            dfw.append(record);
        }

        dfw.flush();
        dfw.close();

        return new ByteArrayInputStream(baos.toByteArray());

    }


}
