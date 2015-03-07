package cascading.avro.conversion;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.specific.SpecificData;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.Serializable;

public abstract class AvroConverterBase<TFrom, TTo> implements AvroConverter<TFrom, TTo>, Serializable {
    public Object convert(Object obj, Schema schema, int toDepth) {
        if (obj == null) {
            return null;
        }
        switch (schema.getType()) {
            case RECORD:
                // We allow a nested record to possibly already be of the destination type TTo,
                // otherwise we must have an object of type TFrom
                if (this.isRecordToType(obj) || toDepth == 0) return obj;
                return this.convertRecord((TFrom) obj, schema, toDepth);

            case UNION:
                return this.convertUnion(obj, schema, toDepth);

            case ARRAY:
                return this.convertArray(obj, schema, toDepth);

            case STRING:
                return this.convertString(obj);

            case ENUM:
                return this.convertEnum(obj, schema);

            case FIXED:
                return this.convertFixed(obj, schema);

            case BYTES:
                return this.convertBytes(obj);

            case MAP:
                return this.convertMap(obj, schema, toDepth);

            case NULL:
                return this.convertNull(obj);

            case BOOLEAN:
                return this.convertBoolean(obj);

            case DOUBLE:
                return this.convertDouble(obj);

            case FLOAT:
                return this.convertFloat(obj);

            case INT:
                return this.convertInt(obj);

            case LONG:
                return this.convertLong(obj);

            default:
                throw new RuntimeException("Can't convert from type " + schema.getType().toString());
        }
    }

    @Override
    public TTo convertRecord(TTo reuse, TFrom obj, Schema schema, int toDepth) {
        throw new NotImplementedException();
    }


    protected Object convertInt(Object obj) {
        return obj;
    }

    protected Object convertLong(Object obj) {
        return obj;
    }

    protected Object convertFloat(Object obj) {
        return obj;
    }

    protected Object convertDouble(Object obj) {
        return obj;
    }

    protected Object convertBoolean(Object obj) {
        return obj;
    }

    protected Object convertNull(Object obj) {
        return obj;
    }

    protected Object convertEnum(Object obj, Schema schema) {
        return obj.toString();
    }

    protected Object convertString(Object obj) {
        return obj.toString();
    }

    /**
     * Is obj of destination type TTo?
     * <p/>
     * If not for type erasure we could ordinarily use TTo.class.isAssignableFrom(obj.getClass()) here.
     *
     * @param obj
     * @return
     */
    protected abstract boolean isRecordToType(Object obj);

    protected abstract Object convertMap(Object obj, Schema schema, int toDepth);

    protected abstract Object convertBytes(Object obj);

    protected abstract Object convertFixed(Object obj, Schema schema);

    protected abstract Object convertArray(Object obj, Schema schema, int toDepth);

    protected abstract Object convertUnion(Object obj, Schema schema, int toDepth);

    protected GenericContainer createSpecificContainer(Schema schema) {
        Class specificClass = SpecificData.get().getClass(schema);
        if (specificClass != null) {
            try {
                return (GenericContainer) specificClass.newInstance();
            }
            catch (Exception e) {
                throw new AvroRuntimeException(String.format("Could not create instance of SpecificData class: '%s'",
                    specificClass));
            }
        }
        return null;
    }
}
