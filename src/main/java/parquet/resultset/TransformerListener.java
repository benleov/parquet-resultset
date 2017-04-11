package parquet.resultset;

import java.io.File;
import java.sql.ResultSet;

/**
 *
 */
public interface TransformerListener {

    void onRecordParsed(ResultSet resultSet);

    void onComplete(File out);

    void onSchemaParsed(SchemaResults schemaResults);
}
