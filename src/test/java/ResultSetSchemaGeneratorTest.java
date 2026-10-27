import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import parquet.resultset.ResultSetSchemaGenerator;
import parquet.resultset.SchemaResults;
import parquet.resultset.SchemaSqlMapping;

import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class ResultSetSchemaGeneratorTest {

    @Test
    public void testGenerateSchema() throws SQLException {

        String schemaName = "SchemaName";
        String namespace = "org.namespace";

        ResultSetSchemaGenerator generator = new ResultSetSchemaGenerator();

        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        when(resultSet.getMetaData()).thenReturn(metaData);

        // mock metadata so schema is created
        when(metaData.getColumnCount()).thenReturn(5);
        when(metaData.getColumnName(1)).thenReturn("id");
        when(metaData.getColumnType(1)).thenReturn(Types.INTEGER);

        when(metaData.getColumnName(2)).thenReturn("name");
        when(metaData.getColumnType(2)).thenReturn(Types.VARCHAR);

        when(metaData.getColumnName(3)).thenReturn("timestamp");
        when(metaData.getColumnType(3)).thenReturn(Types.TIMESTAMP);

        when(metaData.getColumnName(4)).thenReturn("double");
        when(metaData.getColumnType(4)).thenReturn(Types.DOUBLE);

        when(metaData.getColumnName(5)).thenReturn("ID_ACCOUNT");
        when(metaData.getColumnType(5)).thenReturn(Types.NUMERIC);
        when(metaData.getPrecision(5)).thenReturn(9);

        SchemaResults schemaResults = generator.generateSchema(resultSet, schemaName, namespace);

        List<SchemaSqlMapping> mappings = schemaResults.getMappings();

        assertEquals(5, mappings.size(), "Number of column mappings parsed does not match what was expected.");

        assertEquals("id", mappings.get(0).getSqlColumnName());
        assertEquals(Types.INTEGER, mappings.get(0).getSqlType());
        assertEquals(Schema.Type.INT, mappings.get(0).getSchemaType());

        assertEquals("name", mappings.get(1).getSqlColumnName());
        assertEquals(Types.VARCHAR, mappings.get(1).getSqlType());
        assertEquals(Schema.Type.STRING, mappings.get(1).getSchemaType());

        assertEquals("timestamp", mappings.get(2).getSqlColumnName());
        assertEquals(Types.TIMESTAMP, mappings.get(2).getSqlType());
        assertEquals(Schema.Type.LONG, mappings.get(2).getSchemaType());

        assertEquals("double", mappings.get(3).getSqlColumnName());
        assertEquals(Types.DOUBLE, mappings.get(3).getSqlType());
        assertEquals(Schema.Type.DOUBLE, mappings.get(3).getSchemaType());

        // schema field names are lower cased, the sql column name is kept as is
        assertEquals("ID_ACCOUNT", mappings.get(4).getSqlColumnName());
        assertEquals("id_account", mappings.get(4).getSchemaName());
        assertEquals(Schema.Type.INT, mappings.get(4).getSchemaType());

        Schema schema = schemaResults.getParsedSchema();
        assertEquals(schemaName, schema.getName());
        assertEquals(namespace, schema.getNamespace());

        // every field is a union of its type and null, so NULL column values can be written
        for (SchemaSqlMapping mapping : mappings) {
            Schema fieldSchema = schema.getField(mapping.getSchemaName()).schema();

            assertEquals(Schema.Type.UNION, fieldSchema.getType());
            assertEquals(Arrays.asList(Schema.create(mapping.getSchemaType()), Schema.create(Schema.Type.NULL)),
                    fieldSchema.getTypes());
        }
    }

    @Test
    public void testGenerateSchemaWithoutResultSet() throws SQLException {

        SchemaResults schemaResults = new ResultSetSchemaGenerator().generateSchema(null, "SchemaName", null);

        assertEquals(0, schemaResults.getMappings().size());
        assertEquals(0, schemaResults.getParsedSchema().getFields().size());
    }

    @ParameterizedTest
    @CsvSource({
            "BOOLEAN, BOOLEAN",
            "TINYINT, INT", "SMALLINT, INT", "INTEGER, INT",
            "BIGINT, LONG",
            "ROWID, STRING", "CHAR, STRING", "VARCHAR, STRING", "LONGVARCHAR, STRING",
            "NCHAR, STRING", "NVARCHAR, STRING", "LONGNVARCHAR, STRING", "SQLXML, STRING",
            "REAL, FLOAT",
            "DOUBLE, DOUBLE", "FLOAT, DOUBLE", "DECIMAL, DOUBLE", "NUMERIC, DOUBLE",
            "DATE, LONG", "TIME, LONG", "TIMESTAMP, LONG", "TIME_WITH_TIMEZONE, LONG",
            "TIMESTAMP_WITH_TIMEZONE, LONG",
            "BINARY, BYTES", "VARBINARY, BYTES", "LONGVARBINARY, BYTES", "BLOB, BYTES", "CLOB, BYTES",
            "ARRAY, BYTES", "STRUCT, BYTES", "OTHER, BYTES",
    })
    public void testParseSchemaType(JDBCType sqlType, Schema.Type expected) {
        assertEquals(expected, new ResultSetSchemaGenerator().parseSchemaType(sqlType.getVendorTypeNumber()));
    }

    @Test
    public void testParseUnknownSchemaType() {
        assertNull(new ResultSetSchemaGenerator().parseSchemaType(Integer.MIN_VALUE));
    }

    @ParameterizedTest
    @CsvSource({
            "NUMERIC, 9, 0, INT",
            "DECIMAL, 5, 0, INT",
            "NUMERIC, 10, 0, LONG",
            "NUMERIC, 18, 0, LONG",
            "NUMERIC, 19, 0, DOUBLE",
            "DECIMAL, 23, 5, DOUBLE",
            // NUMERIC without precision (e.g. postgres "numeric", oracle "NUMBER") stays a double
            "NUMERIC, 0, 0, DOUBLE",
            "NUMERIC, 0, -127, DOUBLE",
            // precision and scale only change NUMERIC and DECIMAL
            "INTEGER, 10, 0, INT",
            "VARCHAR, 9, 0, STRING",
    })
    public void testParseSchemaTypeWithPrecision(JDBCType sqlType, int precision, int scale, Schema.Type expected) {
        assertEquals(expected,
                new ResultSetSchemaGenerator().parseSchemaType(sqlType.getVendorTypeNumber(), precision, scale));
    }
}
