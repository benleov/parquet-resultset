import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
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
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class ResultSetSchemaGeneratorTest {

    private final ResultSetSchemaGenerator generator = new ResultSetSchemaGenerator();

    @Test
    public void testGenerateSchema() throws SQLException {

        String schemaName = "SchemaName";
        String namespace = "org.namespace";

        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        when(resultSet.getMetaData()).thenReturn(metaData);

        // mock metadata so schema is created
        when(metaData.getColumnCount()).thenReturn(6);
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

        // the label (SELECT amount AS total) is used rather than the column name
        when(metaData.getColumnName(6)).thenReturn("amount");
        when(metaData.getColumnLabel(6)).thenReturn("total");
        when(metaData.getColumnType(6)).thenReturn(Types.DECIMAL);
        when(metaData.getPrecision(6)).thenReturn(23);
        when(metaData.getScale(6)).thenReturn(5);

        SchemaResults schemaResults = generator.generateSchema(resultSet, schemaName, namespace);

        List<SchemaSqlMapping> mappings = schemaResults.getMappings();

        assertEquals(6, mappings.size(), "Number of column mappings parsed does not match what was expected.");

        assertEquals("id", mappings.get(0).getSqlColumnName());
        assertEquals(1, mappings.get(0).getColumnIndex());
        assertEquals(Types.INTEGER, mappings.get(0).getSqlType());
        assertEquals(Schema.Type.INT, mappings.get(0).getSchemaType());

        assertEquals("name", mappings.get(1).getSqlColumnName());
        assertEquals(Types.VARCHAR, mappings.get(1).getSqlType());
        assertEquals(Schema.Type.STRING, mappings.get(1).getSchemaType());

        assertEquals("timestamp", mappings.get(2).getSqlColumnName());
        assertEquals(Types.TIMESTAMP, mappings.get(2).getSqlType());
        assertEquals(Schema.Type.LONG, mappings.get(2).getSchemaType());
        assertEquals(LogicalTypes.localTimestampMillis(), mappings.get(2).getFieldSchema().getLogicalType());

        assertEquals("double", mappings.get(3).getSqlColumnName());
        assertEquals(Types.DOUBLE, mappings.get(3).getSqlType());
        assertEquals(Schema.Type.DOUBLE, mappings.get(3).getSchemaType());

        // schema field names are lower cased, the sql column name is kept as is
        assertEquals("ID_ACCOUNT", mappings.get(4).getSqlColumnName());
        assertEquals("id_account", mappings.get(4).getSchemaName());
        assertEquals(Schema.Type.INT, mappings.get(4).getSchemaType());

        assertEquals("total", mappings.get(5).getSqlColumnName());
        assertEquals("total", mappings.get(5).getSchemaName());
        assertEquals(6, mappings.get(5).getColumnIndex());
        assertEquals(LogicalTypes.decimal(23, 5), mappings.get(5).getFieldSchema().getLogicalType());

        Schema schema = schemaResults.getParsedSchema();
        assertEquals(schemaName, schema.getName());
        assertEquals(namespace, schema.getNamespace());

        // every field is a union of its type and null, so NULL column values can be written
        for (SchemaSqlMapping mapping : mappings) {
            Schema fieldSchema = schema.getField(mapping.getSchemaName()).schema();

            assertEquals(Schema.Type.UNION, fieldSchema.getType());
            assertEquals(Arrays.asList(mapping.getFieldSchema(), Schema.create(Schema.Type.NULL)),
                    fieldSchema.getTypes());
        }
    }

    @Test
    public void testGenerateSchemaWithoutResultSet() throws SQLException {

        SchemaResults schemaResults = generator.generateSchema(null, "SchemaName", null);

        assertEquals(0, schemaResults.getMappings().size());
        assertEquals(0, schemaResults.getParsedSchema().getFields().size());
    }

    @Test
    public void testDuplicateColumnNames() throws SQLException {

        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        when(resultSet.getMetaData()).thenReturn(metaData);

        // e.g. SELECT * FROM a JOIN b, where both tables have an id column
        String[] labels = {"id", "ID", "id", "id_2"};
        when(metaData.getColumnCount()).thenReturn(labels.length);

        for (int x = 0; x < labels.length; x++) {
            when(metaData.getColumnLabel(x + 1)).thenReturn(labels[x]);
            when(metaData.getColumnType(x + 1)).thenReturn(Types.INTEGER);
        }

        List<SchemaSqlMapping> mappings = generator.generateSchema(resultSet, "SchemaName", null).getMappings();

        assertEquals("id", mappings.get(0).getSchemaName());
        assertEquals("id_2", mappings.get(1).getSchemaName());
        assertEquals("id_3", mappings.get(2).getSchemaName());
        assertEquals("id_2_2", mappings.get(3).getSchemaName());
    }

    @ParameterizedTest
    @CsvSource(value = {
            "id, id",
            "ID_ACCOUNT, id_account",
            "COUNT(*), count___",
            "first name, first_name",
            "unit-price, unit_price",
            "1st, _1st",
            "_private, _private",
            "café, caf_",
            "'', _",
    })
    public void testToFieldName(String columnName, String expected) {
        assertEquals(expected, generator.toFieldName(columnName));
    }

    @Test
    public void testToFieldNameIgnoresLocale() {

        Locale defaultLocale = Locale.getDefault();

        try {
            // in turkish "I".toLowerCase() is a dotless i, which is not a valid field name character
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            assertEquals("id", generator.toFieldName("ID"));
        } finally {
            Locale.setDefault(defaultLocale);
        }
    }

    @ParameterizedTest
    @CsvSource({
            "BOOLEAN, BOOLEAN", "BIT, BOOLEAN",
            "TINYINT, INT", "SMALLINT, INT", "INTEGER, INT",
            "BIGINT, LONG",
            "ROWID, STRING", "CHAR, STRING", "VARCHAR, STRING", "LONGVARCHAR, STRING",
            "NCHAR, STRING", "NVARCHAR, STRING", "LONGNVARCHAR, STRING", "SQLXML, STRING",
            "CLOB, STRING", "NCLOB, STRING", "OTHER, STRING", "ARRAY, STRING", "STRUCT, STRING",
            "JAVA_OBJECT, STRING", "DISTINCT, STRING", "DATALINK, STRING", "NULL, STRING",
            "REAL, FLOAT",
            "DOUBLE, DOUBLE", "FLOAT, DOUBLE", "DECIMAL, DOUBLE", "NUMERIC, DOUBLE",
            "DATE, INT", "TIME, INT", "TIME_WITH_TIMEZONE, INT",
            "TIMESTAMP, LONG", "TIMESTAMP_WITH_TIMEZONE, LONG",
            "BINARY, BYTES", "VARBINARY, BYTES", "LONGVARBINARY, BYTES", "BLOB, BYTES",
    })
    public void testParseSchemaType(JDBCType sqlType, Schema.Type expected) {
        assertEquals(expected, generator.parseSchemaType(sqlType.getVendorTypeNumber()));
    }

    @Test
    public void testParseUnknownSchemaType() {
        assertEquals(Schema.Type.STRING, generator.parseSchemaType(Integer.MIN_VALUE));
    }

    @ParameterizedTest
    @CsvSource({
            "NUMERIC, 9, 0, INT",
            "DECIMAL, 5, 0, INT",
            "NUMERIC, 10, 0, LONG",
            "NUMERIC, 18, 0, LONG",
            // decimals are written as bytes with a decimal logical type
            "NUMERIC, 19, 0, BYTES",
            "DECIMAL, 23, 5, BYTES",
            "DECIMAL, 38, 10, BYTES",
            // NUMERIC without precision (e.g. postgres "numeric", oracle "NUMBER") or above 38 is a double
            "NUMERIC, 0, 0, DOUBLE",
            "NUMERIC, 0, -127, DOUBLE",
            "DECIMAL, 39, 0, DOUBLE",
            "DECIMAL, 65, 30, DOUBLE",
            // BIT(n) is a bit string rather than a boolean
            "BIT, 1, 0, BOOLEAN",
            "BIT, 8, 0, BYTES",
            // precision and scale only change NUMERIC, DECIMAL and BIT
            "INTEGER, 10, 0, INT",
            "VARCHAR, 9, 0, STRING",
    })
    public void testParseSchemaTypeWithPrecision(JDBCType sqlType, int precision, int scale, Schema.Type expected) {
        assertEquals(expected, generator.parseSchemaType(sqlType.getVendorTypeNumber(), precision, scale));
    }

    @Test
    public void testParseFieldSchemaLogicalTypes() {

        assertEquals(LogicalTypes.decimal(23, 5), logicalType(Types.DECIMAL, 23, 5));
        assertEquals(LogicalTypes.decimal(30, 0), logicalType(Types.NUMERIC, 30, 0));
        assertEquals(LogicalTypes.date(), logicalType(Types.DATE, 0, 0));
        assertEquals(LogicalTypes.timeMillis(), logicalType(Types.TIME, 0, 0));
        assertEquals(LogicalTypes.timeMillis(), logicalType(Types.TIME_WITH_TIMEZONE, 0, 0));
        assertEquals(LogicalTypes.localTimestampMillis(), logicalType(Types.TIMESTAMP, 0, 0));
        assertEquals(LogicalTypes.timestampMillis(), logicalType(Types.TIMESTAMP_WITH_TIMEZONE, 0, 0));

        assertNull(logicalType(Types.NUMERIC, 9, 0));
        assertNull(logicalType(Types.NUMERIC, 0, 0));
        assertNull(logicalType(Types.VARCHAR, 0, 0));
    }

    private LogicalType logicalType(int sqlType, int precision, int scale) {
        return generator.parseFieldSchema(sqlType, precision, scale).getLogicalType();
    }
}
