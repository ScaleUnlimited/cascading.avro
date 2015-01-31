package cascading.avro;

import org.apache.avro.Schema;

public class ShallowAvroScheme extends AvroScheme {
    /**
     * This Scheme only expands an Avro object to a TupleEntry of its top-level fields, without expanding other Avro
     * fields to further nested TupleEntries.
     */
    public ShallowAvroScheme(Schema schema) {
        super(schema, 1);
    }
}
