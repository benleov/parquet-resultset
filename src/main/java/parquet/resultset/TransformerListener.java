package parquet.resultset;

import java.sql.ResultSet;

/**
 *
 */
public interface TransformerListener {

    void onRecordParsed(ResultSet resultSet);
}
