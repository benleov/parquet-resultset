package parquet.resultset;

import org.apache.avro.Schema;
import java.util.List;

/**
 * Contains both the {@link org.apache.avro.Schema } and the mapping between the sql column name and type,
 * and associated schema type.
 */
public class SchemaResults {

    private Schema parsedSchema;

    private List<SchemaSqlMapping> mappings;

    public Schema getParsedSchema() {
        return parsedSchema;
    }

    public void setParsedSchema(Schema parsedSchema) {
        this.parsedSchema = parsedSchema;
    }

    public List<SchemaSqlMapping> getMappings() {
        return mappings;
    }

    public void setMappings(List<SchemaSqlMapping> mappings) {
        this.mappings = mappings;
    }
}
