package cascading.avro.conversion;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.specific.SpecificData;
import org.apache.commons.collections.MultiMap;
import org.apache.hadoop.io.BytesWritable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Provides mappings to and from Java classes to Avro Schema.Type's. Passed into CascadingToAvro on initialization.
 * <p/>
 * The order of keys and values in toAvroTypeMultiMap and fromAvroTypeMultimap determine the priority given to
 * mappings those relations specify. Highest priority first.
 * <p/>
 * To provide custom mappings subclass this class or implement TypeMapping, providing a parameter-less constructor
 * for initialization after deserialization.
 */
public class TypeMappings implements TypeMapping {
    // Provides stable iteration order, note that classes placed higher in the list take precedence
    public static final ImmutableSetMultimap<Class<?>, Schema.Type> DEFAULT_AVRO_TYPE_MULTIMAP =
        new ImmutableSetMultimap.Builder<Class<?>, Schema.Type>()
            .put(TupleEntry.class, Schema.Type.RECORD)
            .putAll(Tuple.class, Schema.Type.MAP, Schema.Type.ARRAY)
            .putAll(Iterable.class, Schema.Type.MAP, Schema.Type.ARRAY)
            .put(List.class, Schema.Type.ARRAY)
            .put(Map.class, Schema.Type.MAP)
            .put(BytesWritable.class, Schema.Type.BYTES)
            .put(byte[].class, Schema.Type.BYTES)
            .put(Enum.class, Schema.Type.ENUM)
            .put(GenericFixed.class, Schema.Type.FIXED)
            .put(Integer.class, Schema.Type.INT)
            .put(Long.class, Schema.Type.LONG)
            .put(Boolean.class, Schema.Type.BOOLEAN)
            .put(Double.class, Schema.Type.DOUBLE)
            .put(Float.class, Schema.Type.FLOAT)
            .putAll(String.class, Schema.Type.STRING, Schema.Type.ENUM)
            .build();

    protected final ImmutableSetMultimap<Class<?>, Schema.Type> toAvroTypeMultiMap;

    protected final ImmutableSetMultimap<Schema.Type, Class<?>> fromAvroTypeMultimap;

    public TypeMappings(ImmutableSetMultimap<Class<?>, Schema.Type> toAvroTypeMultiMap) {
        this.toAvroTypeMultiMap = toAvroTypeMultiMap;
        this.fromAvroTypeMultimap = this.toAvroTypeMultiMap.inverse();
    }

    public TypeMappings() {
        this(DEFAULT_AVRO_TYPE_MULTIMAP);
    }

    @Override
    public ImmutableSet<Schema.Type> getDestinationTypes(Class<?> c) {
        // Return early if we have an exact match on the class
        if (toAvroTypeMultiMap.containsKey(c)) return toAvroTypeMultiMap.get(c);
        // Otherwise check if to see if our class can be assigned to each class
        ImmutableSet.Builder<Schema.Type> builder = ImmutableSet.builder();
        for (Class<?> superClass : toAvroTypeMultiMap.keySet()) {
            if (superClass.isAssignableFrom(c)) {
                builder.addAll(toAvroTypeMultiMap.get(superClass));
            }
        }
        return builder.build();
    }

    @Override
    public boolean isMappable(Class<?> c, Schema.Type t) {
        for (Class<?> superClass : fromAvroTypeMultimap.get(t)) {
            if (superClass.isAssignableFrom(c)) return true;
        }
        try {
            return SpecificData.get().getSchema(c).getType().equals(t);
        }
        catch (AvroRuntimeException e) {
            return false;
        }
    }

    public static HashMap asMap(Object tuple) {
        return asMap((Tuple) tuple, 0);
    }

    public static HashMap asMapDeep(Object tuple) {
        return asMap((Tuple) tuple, -1);
    }

    @SuppressWarnings({"unchecked"})
    public static HashMap asMap(Tuple tuple, int depth) {
        if (tuple.size() % 2 != 0)
            throw new IllegalArgumentException("Cannot convert Tuple to map; odd number of elements.");
        HashMap map = new HashMap(tuple.size() / 2);
        for (int i = 0; i < tuple.size(); i += 2) {
            Object value = tuple.getObject(i + 1);
            map.put(tuple.getObject(i), depth != 0 && value instanceof Tuple ? asMap((Tuple) value, depth - 1) : value);
        }
        return map;
    }
}
