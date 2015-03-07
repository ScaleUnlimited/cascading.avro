package cascading.avro;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.apache.avro.Schema;
import org.apache.hadoop.mapred.JobConf;

public class PackedAvroScheme extends AvroScheme {
    /**
     * This scheme should be used when you don't want cascading.avro to automatically unpack or pack your Avro objects.
     * The constructors are similar to the super class but there is only ever one field incoming or outgoing. The parameter
     * is the type of Avro record to read.
     */
    public PackedAvroScheme() {
        this(null);
    }

    /**
     * Constructs a scheme from an Avro schema and names the single field using the Schema name.
     *
     * @param schema The avro schema to use
     */
    public PackedAvroScheme(Schema schema) {
        super(schema, 0);
    }

    @Override
    public Fields retrieveSourceFields(FlowProcess<JobConf> flowProcess, Tap tap) {
        if (this.schema == null) {
            this.setSourceFields(Fields.UNKNOWN);
        }
        else {
            this.setSourceFields(new Fields(new Comparable[]{this.schema.getName()}));
        }

        return this.getSourceFields();
    }
}
