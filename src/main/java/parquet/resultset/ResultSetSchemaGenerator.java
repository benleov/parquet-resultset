package parquet.resultset;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Parses a {@link ResultSet } and generates {@link SchemaResults } which contains the mappings of sql columns
 * to Schema fields.
 */
public class ResultSetSchemaGenerator {

    /**
     * Largest decimal precision written as a decimal logical type. Larger or unknown precisions are written as
     * doubles. 38 is the maximum supported by Athena, Hive and Spark.
     */
    public static final int MAX_DECIMAL_PRECISION = 38;

    /**
     * Generates parquet schema using {@link ResultSetMetaData }
     *
     * @param resultSet
     * @param name Record name.
     * @param nameSpace
     * @return
     * @throws SQLException
     */
    public SchemaResults generateSchema(ResultSet resultSet, String name, String nameSpace) throws SQLException {

        SchemaResults schemaResults = new SchemaResults();
        List<SchemaSqlMapping> mappings = new ArrayList<>();

        Schema recordSchema = Schema.createRecord(name, null, nameSpace, false);

        List<Schema.Field> fields = new ArrayList<>();

        if (resultSet != null) {

            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();
            Set<String> usedNames = new HashSet<>();

            for (int x = 1; x <= columnCount; x++) {

                // the label is the "AS" alias, which drivers return as the column name when there is none
                String columnName = resultSetMetaData.getColumnLabel(x);

                if (columnName == null || columnName.isEmpty()) {
                    columnName = resultSetMetaData.getColumnName(x);
                }

                int sqlColumnType = resultSetMetaData.getColumnType(x);
                int precision = resultSetMetaData.getPrecision(x);
                int scale = resultSetMetaData.getScale(x);
                boolean signed = isSigned(resultSetMetaData, x, sqlColumnType);
                String typeName = sqlColumnType == Types.ARRAY ? resultSetMetaData.getColumnTypeName(x) : null;
                String schemaName = uniqueName(toFieldName(columnName), usedNames);

                Schema fieldSchema = parseFieldSchema(sqlColumnType, precision, scale, signed, typeName);
                mappings.add(new SchemaSqlMapping(x, schemaName, columnName, sqlColumnType, fieldSchema));
                fields.add(createNullableField(recordSchema, schemaName, fieldSchema));
            }
        }

        recordSchema.setFields(fields);

        schemaResults.setMappings(mappings);
        schemaResults.setParsedSchema(recordSchema);

        return schemaResults;
    }

    /**
     * Converts an sql column name into a valid schema field name: lower case, with characters that are not
     * allowed in a field name (anything other than letters, digits and underscores) replaced by underscores,
     * and an underscore prefix when it would otherwise start with a digit. e.g. "COUNT(*)" becomes "count___".
     *
     * @param columnName The sql column name or label.
     * @return
     */
    public String toFieldName(String columnName) {

        String lowerCase = columnName == null ? "" : columnName.toLowerCase(Locale.ROOT);
        StringBuilder fieldName = new StringBuilder(lowerCase.length() + 1);

        for (char c : lowerCase.toCharArray()) {
            boolean valid = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_';
            fieldName.append(valid ? c : '_');
        }

        if (fieldName.length() == 0 || Character.isDigit(fieldName.charAt(0))) {
            fieldName.insert(0, '_');
        }

        return fieldName.toString();
    }

    /**
     * Whether an INTEGER or BIGINT column is signed. Only those are affected, as unsigned TINYINT and SMALLINT
     * (e.g. mysql UNSIGNED, sql server tinyint) still fit in an int. Assumes signed if the driver can't say.
     */
    private static boolean isSigned(ResultSetMetaData resultSetMetaData, int column, int sqlColumnType) {

        if (sqlColumnType != Types.INTEGER && sqlColumnType != Types.BIGINT) {
            return true;
        }

        try {
            return resultSetMetaData.isSigned(column);
        } catch (SQLException e) {
            return true;
        }
    }

    /**
     * Appends _2, _3 etc. to the name when it has already been used, e.g. for the two "id" columns of a join.
     */
    private static String uniqueName(String name, Set<String> usedNames) {

        String uniqueName = name;

        for (int suffix = 2; !usedNames.add(uniqueName); suffix++) {
            uniqueName = name + "_" + suffix;
        }

        return uniqueName;
    }

    /**
     * Converts sql column type to the schema of a field, including any logical type:
     *
     * <ul>
     *     <li>NUMERIC and DECIMAL whole numbers (scale 0) to INT (precision up to 9) or LONG (up to 18),
     *     otherwise a decimal with the same precision and scale. Doubles are used when the precision is unknown
     *     (e.g. oracle NUMBER, postgres numeric) or above {@link #MAX_DECIMAL_PRECISION}.</li>
     *     <li>DATE to date, TIME to time-millis, TIMESTAMP to local-timestamp-millis (no time zone) and
     *     TIMESTAMP_WITH_TIMEZONE to timestamp-millis (a UTC instant).</li>
     *     <li>Unsigned INTEGER to LONG and unsigned BIGINT to decimal(20, 0), e.g. mysql INT UNSIGNED.</li>
     *     <li>Binary types and BLOB to BYTES, and BIT to BOOLEAN when it is a single bit.</li>
     *     <li>ARRAY to an array of nullable elements, see {@link #parseArrayElementType(String)}.</li>
     *     <li>Everything else, including CLOB and OTHER (e.g. postgres uuid and json), to STRING.</li>
     * </ul>
     *
     * @param sqlColumnType {@link java.sql.Types }
     * @param precision     column precision from {@link ResultSetMetaData#getPrecision(int)}
     * @param scale         column scale from {@link ResultSetMetaData#getScale(int)}
     * @param signed        whether the column is signed, from {@link ResultSetMetaData#isSigned(int)}
     * @param typeName      the database type name from {@link ResultSetMetaData#getColumnTypeName(int)}, which
     *                      gives the element type of ARRAY columns. May be null.
     * @return
     */
    public Schema parseFieldSchema(int sqlColumnType, int precision, int scale, boolean signed, String typeName) {

        if (!signed && sqlColumnType == Types.INTEGER) {
            return Schema.create(Schema.Type.LONG);         // up to 4,294,967,295
        }

        if (!signed && sqlColumnType == Types.BIGINT) {
            return LogicalTypes.decimal(20, 0).addToSchema(Schema.create(Schema.Type.BYTES));  // up to 2^64 - 1
        }

        if (sqlColumnType == Types.ARRAY) {
            Schema elementSchema = Schema.createUnion(
                    Schema.create(parseArrayElementType(typeName)), Schema.create(Schema.Type.NULL));
            return Schema.createArray(elementSchema);
        }

        return parseScalarSchema(sqlColumnType, precision, scale);
    }

    /**
     * Converts the database type name of an ARRAY column into the schema type of its elements. JDBC has no
     * standard way to get the element type, so this recognises the common forms of type name: postgres
     * ("_int4", "_text"), the sql standard used by H2 and HSQLDB ("INTEGER ARRAY") and hive/spark
     * ("array&lt;bigint&gt;"). Booleans, whole numbers and floating point numbers keep their type. Decimals are
     * DOUBLE, as their scale is unknown, and anything else, including unrecognised type names, is STRING.
     *
     * @param typeName The database type name, may be null.
     * @return
     */
    public Schema.Type parseArrayElementType(String typeName) {

        if (typeName == null) {
            return Schema.Type.STRING;
        }

        String name = typeName.trim().toLowerCase(Locale.ROOT);

        if (name.startsWith("_")) {
            name = name.substring(1);
        } else if (name.endsWith(" array")) {
            name = name.substring(0, name.length() - " array".length());
        } else if (name.startsWith("array<") && name.endsWith(">")) {
            name = name.substring("array<".length(), name.length() - 1);
        } else {
            return Schema.Type.STRING;
        }

        // remove any precision, e.g. numeric(10,2)
        int bracket = name.indexOf('(');
        name = (bracket >= 0 ? name.substring(0, bracket) : name).trim();

        switch (name) {
            case "bool":
            case "boolean":
                return Schema.Type.BOOLEAN;
            case "int2":
            case "int4":
            case "tinyint":
            case "smallint":
            case "int":
            case "integer":
                return Schema.Type.INT;
            case "int8":
            case "bigint":
                return Schema.Type.LONG;
            case "float4":
            case "real":
                return Schema.Type.FLOAT;
            case "float8":
            case "float":
            case "double":
            case "double precision":
            case "numeric":
            case "decimal":
                return Schema.Type.DOUBLE;
            default:
                return Schema.Type.STRING;
        }
    }

    /**
     * Converts sql column type to the schema of a field, for a signed column. ARRAY columns have string elements.
     * See {@link #parseFieldSchema(int, int, int, boolean, String)}.
     *
     * @param sqlColumnType {@link java.sql.Types }
     * @param precision     column precision from {@link ResultSetMetaData#getPrecision(int)}
     * @param scale         column scale from {@link ResultSetMetaData#getScale(int)}
     * @return
     */
    public Schema parseFieldSchema(int sqlColumnType, int precision, int scale) {
        return parseFieldSchema(sqlColumnType, precision, scale, true, null);
    }

    private static Schema parseScalarSchema(int sqlColumnType, int precision, int scale) {

        switch (sqlColumnType) {

            case Types.BOOLEAN:
                return Schema.create(Schema.Type.BOOLEAN);

            case Types.BIT:                 // BIT(1) is a boolean, e.g. sql server bit and mysql TINYINT(1)
                return Schema.create(precision <= 1 ? Schema.Type.BOOLEAN : Schema.Type.BYTES);

            case Types.TINYINT:             // 1 byte
            case Types.SMALLINT:            // 2 bytes
            case Types.INTEGER:             // 4 bytes
                return Schema.create(Schema.Type.INT);      // 32 bit (4 bytes) (signed)

            case Types.BIGINT:              // 8 bytes
                return Schema.create(Schema.Type.LONG);     // 64 bit (signed)

            case Types.REAL:                // Approximate numerical (mantissa single precision 7)
                return Schema.create(Schema.Type.FLOAT);    // A 32-bit IEEE single-float

            case Types.DOUBLE:              // Approximate numerical (mantissa precision 16)
            case Types.FLOAT:               // Approximate numerical (mantissa precision 16)
                return Schema.create(Schema.Type.DOUBLE);   // A 64-bit IEEE double-float

            case Types.DECIMAL:             // Exact numerical (5 - 17 bytes)
            case Types.NUMERIC:             // Exact numerical (5 - 17 bytes)
                return parseDecimalSchema(precision, scale);

            case Types.DATE:
                return LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));

            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                return LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));

            case Types.TIMESTAMP:
                return LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG));

            case Types.TIMESTAMP_WITH_TIMEZONE:
                return LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                return Schema.create(Schema.Type.BYTES);    // sequence of bytes

            default:                        // character types, CLOB, SQLXML, ROWID, OTHER, STRUCT etc.
                return Schema.create(Schema.Type.STRING);   // unicode string
        }
    }

    private static Schema parseDecimalSchema(int precision, int scale) {

        if (scale == 0 && precision > 0 && precision <= 9) {
            return Schema.create(Schema.Type.INT);          // up to 999,999,999 fits in a 32 bit int
        }

        if (scale == 0 && precision > 0 && precision <= 18) {
            return Schema.create(Schema.Type.LONG);         // up to 18 digits fits in a 64 bit long
        }

        if (precision > 0 && precision <= MAX_DECIMAL_PRECISION && scale >= 0 && scale <= precision) {
            return LogicalTypes.decimal(precision, scale).addToSchema(Schema.create(Schema.Type.BYTES));
        }

        return Schema.create(Schema.Type.DOUBLE);
    }

    /**
     * Converts sql column type to schema type, using the column's precision and scale. See
     * {@link #parseFieldSchema(int, int, int)} for the full mapping, including logical types.
     *
     * @param sqlColumnType {@link java.sql.Types }
     * @param precision     column precision from {@link ResultSetMetaData#getPrecision(int)}
     * @param scale         column scale from {@link ResultSetMetaData#getScale(int)}
     * @return
     */
    public Schema.Type parseSchemaType(int sqlColumnType, int precision, int scale) {
        return parseFieldSchema(sqlColumnType, precision, scale).getType();
    }

    /**
     * Converts sql column type to schema type. See {@link java.sql.Types } and {@link Schema.Type } for more details.
     *
     * https://www.cis.upenn.edu/~bcpierce/courses/629/jdkdocs/guide/jdbc/getstart/mapping.doc.html
     *
     * @param sqlColumnType
     * @return
     */
    public Schema.Type parseSchemaType(int sqlColumnType) {
        return parseSchemaType(sqlColumnType, 0, 0);
    }


    /**
     * Creates field that can be null {@link Schema#createUnion(List)}
     *
     * @param recordSchema The root record schema
     * @param columnName   The schema column name
     * @param type         Schema type for this field
     * @return
     */
    public Schema.Field createNullableField(Schema recordSchema, String columnName, Schema.Type type) {
        return createNullableField(recordSchema, columnName, Schema.create(type));
    }

    /**
     * Creates field that can be null {@link Schema#createUnion(List)}
     *
     * @param recordSchema The root record schema
     * @param columnName   The schema column name
     * @param fieldSchema  Schema for this field, which may have a logical type
     * @return
     */
    public Schema.Field createNullableField(Schema recordSchema, String columnName, Schema fieldSchema) {

        Schema nullSchema = Schema.create(Schema.Type.NULL);

        List<Schema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(fieldSchema);
        fieldSchemas.add(nullSchema);

        Schema unionSchema = Schema.createUnion(fieldSchemas);

        return new Schema.Field(columnName, unionSchema, null, null);
    }
}
