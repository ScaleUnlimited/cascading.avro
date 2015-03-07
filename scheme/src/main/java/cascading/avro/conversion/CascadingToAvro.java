package cascading.avro.conversion;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.UnresolvedUnionException;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.io.BytesWritable;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;

public class CascadingToAvro extends AvroConverterBase<TupleEntry, IndexedRecord> implements Serializable {
    protected TypeMapping typeMapping;

    public CascadingToAvro(){
        this(new TypeMappings());
    }

    public CascadingToAvro(TypeMapping typeMapping){
        this.typeMapping = typeMapping;
    }

    @Override
    public IndexedRecord convertRecord(TupleEntry tupleEntry, Schema schema, int toDepth) {
        return convertRecord(tupleEntry.getTuple(), schema, toDepth);
    }

    public IndexedRecord convertRecord(Tuple tuple, Schema schema, int toDepth) {
        if (toDepth == 0)
            throw new IllegalArgumentException("Conversion tree too deep");
        IndexedRecord avroRecord;
        avroRecord = this.createAvroRecord(schema);
        if (!(avroRecord.getSchema().getFields().size() == tuple.size())) {
            throw new AvroRuntimeException("Arity mismatch between incoming tuple and schema");
        }
        List<Schema.Field> schemaFields = avroRecord.getSchema().getFields();
        for (int i = 0; i < schemaFields.size(); i++) {
            avroRecord.put(i, convert(tuple.getObject(i), schemaFields.get(i).schema(), toDepth - 1));
        }
        return avroRecord;
    }

    @Override
    public Object convertMap(Object obj, Schema schema, int toDepth) {
        Map<String, Object> convertedMap = new HashMap<String, Object>();
        if (obj instanceof Tuple) {
            Schema.Type mapValueType = schema.getValueType().getType();
            Tuple tuple = (Tuple) obj;
            if (tuple.size() % 2 == 0) {
                for (int i = 0; i < tuple.size(); i = i + 2) {
                    if (!CharSequence.class.isAssignableFrom(tuple.getObject(i).getClass())) {
                        throw new AvroRuntimeException(
                            "Invalid map definition - the key should be a CharSequence - instead of "
                                + tuple.getObject(i).getClass());
                    }

                    if (!typeMapping.isMappable(tuple.getObject(i + 1).getClass(), mapValueType)) {
                        throw new AvroRuntimeException(String.format("Found map value with type %s that could not be unified with" +
                                " expected Avro type: %s.%s",
                            tuple.getObject(i + 1).getClass(), Schema.Type.class, mapValueType));
                    }
                    convertedMap.put(tuple.getObject(i).toString(), this.convert(tuple.getObject(i + 1), schema.getValueType(), toDepth));
                }
            }
            else {
                throw new AvroRuntimeException("Can't convert from an odd length tuple to a map");
            }
        }
        else {
            for (Map.Entry<String, Object> e : ((Map<String, Object>) obj).entrySet()) {
                convertedMap.put(e.getKey(), this.convert(e.getValue(), schema.getValueType(), toDepth));
            }
        }
        return convertedMap;
    }

    @Override
    protected boolean isRecordToType(Object obj) {
        return IndexedRecord.class.isAssignableFrom(obj.getClass());
    }

    @Override
    public Object convertEnum(Object obj, Schema schema) {
        return new GenericData.EnumSymbol(schema, obj.toString());
    }

    @Override
    public Object convertBytes(Object obj) {
        BytesWritable bytes = obj instanceof byte[] ? new BytesWritable((byte[]) obj) : (BytesWritable) obj;
        return ByteBuffer.wrap(Arrays.copyOfRange(bytes.getBytes(), 0, bytes.getLength()));
    }

    @Override
    public Object convertFixed(Object obj, Schema schema) {
        if (GenericData.Fixed.class.isAssignableFrom(obj.getClass())) {
            return obj;
        }
        BytesWritable bytes = (BytesWritable) obj;
        GenericData.Fixed fixed = (GenericData.Fixed) createSpecificContainer(schema);
        fixed.bytes(Arrays.copyOfRange(bytes.getBytes(), 0, bytes.getLength()));
        return fixed;
    }

    @Override
    public Object convertArray(Object obj, Schema schema, int toDepth) {
        Schema.Type arrayElementType = schema.getElementType().getType();
        if (obj instanceof Iterable) {
            Schema elementSchema = schema.getElementType();
            List<Object> array = new ArrayList<Object>();
            for (Object element : (Iterable) obj) {
                if (!typeMapping.isMappable(element.getClass(), arrayElementType)) {
                    throw new AvroRuntimeException(String.format("Array element with type %s could not be unified with" +
                            " expected Avro type: %s.%s",
                        element.getClass(), Schema.Type.class, arrayElementType));
                }
                array.add(convert(element, elementSchema, toDepth));
            }

            return new GenericData.Array<Object>(schema, array);
        }

        throw new AvroRuntimeException("Can't convert from non-iterable to array");
    }

    /**
     * We use a heuristic to infer the type of an incoming Object destined for a Union field. It's not perfect because
     * Cascading's use of Tuple(Entry) for records, maps, and arrays presents some ambiguity here, but it should work for
     * most cases.
     *
     * @return
     */
    @Override
    public Object convertUnion(Object obj, Schema schema, int toDepth) {
        AvroRuntimeException innerException = null;
        // Note this outer iteration is only likely to loop once or twice so we favour an early exit rather than
        // building a set of Schema.Types in the union
        for (Schema.Type type : typeMapping.getDestinationTypes(obj.getClass())) {
            for (Schema unionElementSchema : schema.getTypes()) {
                if (unionElementSchema.getType().equals(type)) {
                    try {
                        return convert(obj, unionElementSchema, toDepth);
                    }
                    catch (AvroRuntimeException e) {
                        innerException = e;
                        continue;
                    }
                }
            }
        }
        if (innerException != null) {
            throw new AvroRuntimeException(
                String.format("Failed to convert object of type %s to union: %s", obj.getClass(), schema),
                innerException);
        }

        throw new UnresolvedUnionException(schema, obj);
    }

    protected IndexedRecord createAvroRecord(Schema schema) {
        GenericContainer specificContainer = createSpecificContainer(schema);
        return specificContainer != null ? (IndexedRecord) specificContainer : new GenericData.Record(schema);
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeObject(this.typeMapping.getClass());
    }

    private void readObject(java.io.ObjectInputStream in) throws Exception {
        this.typeMapping = (TypeMapping) ((Class)in.readObject()).newInstance();
    }
}
