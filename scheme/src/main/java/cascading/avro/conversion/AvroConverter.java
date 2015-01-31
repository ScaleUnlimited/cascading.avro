package cascading.avro.conversion;

import org.apache.avro.Schema;

/**
 * This interfaces defines a conversion between Avro schema representing objects by pivoting on the Avro schema.
 * <p/>
 * The conversion is parameterised by the types representing Avro records on either side of the conversion mapping.
 */
public interface AvroConverter<TRecordFrom, TRecordTo> {
    TRecordTo convertRecord(TRecordFrom obj, Schema schema, int toDepth);

    TRecordTo convertRecord(TRecordTo reuse, TRecordFrom obj, Schema schema, int toDepth);

    Object convert(Object obj, Schema schema, int toDepth);
}
