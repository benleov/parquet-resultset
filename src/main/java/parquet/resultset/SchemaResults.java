package parquet.resultset;

import org.apache.avro.Schema;
import java.util.List;

/**
 *
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

    public void setMappings(List<SchemaSqlMapping> mappings) {
        this.mappings = mappings;
    }

    public List<SchemaSqlMapping> getMappings() {
        return mappings;
    }
}
