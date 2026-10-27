import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import parquet.resultset.ResultSetSchemaGenerator;
import parquet.resultset.SchemaResults;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Transforms a real result set from an in memory H2 database, to check the driver's metadata and values rather than
 * those of a fake result set.
 */
public class H2IntegrationTest {

    /**
     * Daylight saving starts in New York at 2am, so 2:30am doesn't exist in that zone. The test JVM runs in it, see
     * build.gradle.
     */
    private static final LocalDateTime SKIPPED_HOUR = LocalDateTime.of(2026, 3, 8, 2, 30, 0, 123_000_000);

    private static final String QUERY = "SELECT id AS widget_id, flag, big, price, ratio, name, data, day_col, "
            + "time_col, created, created_tz, ids, tags FROM widget ORDER BY id";

    private static Connection connection;

    @BeforeAll
    public static void createDatabase() throws SQLException {

        connection = DriverManager.getConnection("jdbc:h2:mem:widgets");

        try (Statement statement = connection.createStatement()) {

            statement.execute("CREATE TABLE widget (id INT PRIMARY KEY, flag BOOLEAN, big BIGINT, "
                    + "price NUMERIC(10, 2), ratio DOUBLE PRECISION, name VARCHAR(20), data VARBINARY(10), "
                    + "day_col DATE, time_col TIME(3), created TIMESTAMP(3), created_tz TIMESTAMP(3) WITH TIME ZONE, "
                    + "ids INTEGER ARRAY, tags VARCHAR(10) ARRAY)");

            statement.execute("INSERT INTO widget VALUES (1, TRUE, 10000000000, 12.5, 0.25, 'widget', X'010203', "
                    + "DATE '2026-03-08', TIME '13:45:30.123', TIMESTAMP '2026-03-08 02:30:00.123', "
                    + "TIMESTAMP WITH TIME ZONE '2026-01-02 03:04:05.678+10:00', ARRAY[1, NULL, 3], ARRAY['a', 'b'])");

            statement.execute("INSERT INTO widget (id) VALUES (2)");

            statement.execute("INSERT INTO widget (id, ids, tags) VALUES (3, ARRAY[], ARRAY[NULL])");
        }
    }

    @AfterAll
    public static void closeDatabase() throws SQLException {
        connection.close();
    }

    /**
     * The transformer tests, for their transformers and readers.
     */
    static Stream<ResultSetTest> transformers() {
        return Stream.of(new ResultSetAvroTransformerTest(), new ResultSetParquetTransformerTest());
    }

    @Test
    public void testSkippedHourIsInTheTestZone() {
        // otherwise the test can't tell whether timestamps are read through the JVM's time zone
        assertTrue(ZoneId.systemDefault().getRules().getValidOffsets(SKIPPED_HOUR).isEmpty(),
                ZoneId.systemDefault().toString());
    }

    @Test
    public void testSchema() throws SQLException {

        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(QUERY)) {

            SchemaResults results = new ResultSetSchemaGenerator().generateSchema(resultSet, "Widget", "test");
            Schema schema = results.getParsedSchema();

            assertEquals(Schema.Type.INT, fieldSchema(schema, "widget_id").getType());
            assertEquals(Schema.Type.BOOLEAN, fieldSchema(schema, "flag").getType());
            assertEquals(Schema.Type.LONG, fieldSchema(schema, "big").getType());
            assertEquals(LogicalTypes.decimal(10, 2), fieldSchema(schema, "price").getLogicalType());
            assertEquals(Schema.Type.DOUBLE, fieldSchema(schema, "ratio").getType());
            assertEquals(Schema.Type.STRING, fieldSchema(schema, "name").getType());
            assertEquals(Schema.Type.BYTES, fieldSchema(schema, "data").getType());
            assertEquals(LogicalTypes.date(), fieldSchema(schema, "day_col").getLogicalType());
            assertEquals(LogicalTypes.timeMillis(), fieldSchema(schema, "time_col").getLogicalType());
            assertEquals(LogicalTypes.localTimestampMillis(), fieldSchema(schema, "created").getLogicalType());
            assertEquals(LogicalTypes.timestampMillis(), fieldSchema(schema, "created_tz").getLogicalType());
            assertEquals(Schema.Type.INT, elementType(fieldSchema(schema, "ids")));
            assertEquals(Schema.Type.STRING, elementType(fieldSchema(schema, "tags")));
        }
    }

    @ParameterizedTest
    @MethodSource("transformers")
    public void testTransform(ResultSetTest test) throws IOException, SQLException {

        List<GenericRecord> records = transform(test);

        assertEquals(3, records.size());

        GenericRecord values = records.get(0);
        assertEquals(1, values.get("widget_id"));
        assertEquals(true, values.get("flag"));
        assertEquals(10000000000L, values.get("big"));
        assertEquals(new BigDecimal("12.50"), values.get("price"));
        assertEquals(0.25d, values.get("ratio"));
        assertEquals("widget", ResultSetTest.normalise(values.get("name")));
        assertEquals(ByteBuffer.wrap(new byte[]{1, 2, 3}), values.get("data"));
        assertEquals(LocalDate.of(2026, 3, 8), values.get("day_col"));
        assertEquals(LocalTime.of(13, 45, 30, 123_000_000), values.get("time_col"));
        assertEquals(SKIPPED_HOUR, values.get("created"));
        assertEquals(Instant.parse("2026-01-01T17:04:05.678Z"), values.get("created_tz"));
        assertEquals(Arrays.asList(1, null, 3), values.get("ids"));
        assertEquals(Arrays.asList("a", "b"), ResultSetTest.normalise(values.get("tags")));

        GenericRecord nulls = records.get(1);
        assertEquals(2, nulls.get("widget_id"));
        for (String field : Arrays.asList("flag", "big", "price", "ratio", "name", "data", "day_col", "time_col",
                "created", "created_tz", "ids", "tags")) {
            assertNull(nulls.get(field), field);
        }

        GenericRecord arrays = records.get(2);
        assertEquals(Collections.emptyList(), arrays.get("ids"));
        assertEquals(Collections.singletonList(null), arrays.get("tags"));
    }

    private static List<GenericRecord> transform(ResultSetTest test) throws IOException, SQLException {

        Path file = Files.createTempFile("h2", null);

        try {
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(QUERY);
                 OutputStream outputStream = Files.newOutputStream(file)) {

                test.createTransformer().transform(resultSet, "Widget", "test", outputStream);
            }

            return test.readRecords(file.toFile());

        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * The schema of a field, without the null union.
     */
    private static Schema fieldSchema(Schema schema, String name) {
        return schema.getField(name).schema().getTypes().get(0);
    }

    private static Schema.Type elementType(Schema arraySchema) {
        return arraySchema.getElementType().getTypes().get(0).getType();
    }
}
