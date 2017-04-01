package parquet.resultset;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class ResultSetParquetTransformer {

    public static final String DEFAULT_TEMP_FILE_PATH = "/tmp/temp.par";

    private InputStream toParquet(ResultSet resultSet, String schemaName, String namespace,
                                  List<TransformerListener> listeners) throws IOException, SQLException {

        SchemaResults schemaResults = new ResultSetSchemaGenerator().generateSchema(resultSet,
                schemaName, namespace);

        MessageType parquetSchema = new AvroSchemaConverter().convert(schemaResults.getParsedSchema());
        AvroWriteSupport writeSupport = new AvroWriteSupport(parquetSchema, schemaResults.getParsedSchema());
        CompressionCodecName compressionCodecName = CompressionCodecName.SNAPPY;

        int blockSize = 256 * 1024 * 1024;
        int pageSize = 64 * 1024;

        Path outputPath = new Path(DEFAULT_TEMP_FILE_PATH);

        final LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());

        // TODO: throw custom exception
        // File file = localFileSystem.pathToFile(outputPath);
        // file.delete();

        ParquetWriter parquetWriter = new ParquetWriter(outputPath,
                writeSupport, compressionCodecName, blockSize, pageSize);

        List<GenericRecord> records = new ArrayList<>();

        while (resultSet.next()) {

            GenericRecordBuilder builder = new GenericRecordBuilder(schemaResults.getParsedSchema());

            for (SchemaSqlMapping mapping : schemaResults.getMappings()) {

                builder.set(
                        schemaResults.getParsedSchema().getField(mapping.getSchemaName()),
                        extractResult(mapping, resultSet));
            }

            records.add(builder.build());

            listeners.forEach(transformerListener -> transformerListener.onRecordParsed(resultSet));
        }

        for (GenericRecord record : records) {
            parquetWriter.write(record);
        }

        parquetWriter.close();

        return new FileInputStream(localFileSystem.pathToFile(outputPath));

    }


    private static Object extractResult(SchemaSqlMapping mapping, ResultSet resultSet) throws SQLException {

        switch (mapping.getSqlType()) {
            case Types.BOOLEAN:
                return resultSet.getBoolean(mapping.getSqlColumnName());
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.ROWID:
                return resultSet.getInt(mapping.getSqlColumnName());
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.SQLXML:
                return resultSet.getString(mapping.getSqlColumnName());
            case Types.REAL:
            case Types.FLOAT:
                return resultSet.getFloat(mapping.getSqlColumnName());
            case Types.DOUBLE:
                return resultSet.getDouble(mapping.getSqlColumnName());
            case Types.NUMERIC:
                return resultSet.getBigDecimal(mapping.getSqlColumnName());
            case Types.DECIMAL:
                return resultSet.getBigDecimal(mapping.getSqlColumnName());
            case Types.DATE:
                return resultSet.getDate(mapping.getSqlColumnName()).getTime();
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                return resultSet.getTime(mapping.getSqlColumnName()).getTime();
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return resultSet.getTimestamp(mapping.getSqlColumnName()).getTime();
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.NULL:
            case Types.OTHER:
            case Types.JAVA_OBJECT:
            case Types.DISTINCT:
            case Types.STRUCT:
            case Types.ARRAY:
            case Types.BLOB:
            case Types.CLOB:
            case Types.REF:
            case Types.DATALINK:
            case Types.NCLOB:
            case Types.REF_CURSOR:
                return resultSet.getByte(mapping.getSqlColumnName());
            default:
                return resultSet.getString(mapping.getSqlColumnName());
        }
    }

}
