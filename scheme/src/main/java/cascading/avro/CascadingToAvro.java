/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.avro;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Fixed;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.io.BytesWritable;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class CascadingToAvro {

    @SuppressWarnings("serial")
    private static Map<Class<?>, Schema.Type> TYPE_MAP = new HashMap<Class<?>, Schema.Type>() {
        {
            put(Integer.class, Schema.Type.INT);
            put(Long.class, Schema.Type.LONG);
            put(Boolean.class, Schema.Type.BOOLEAN);
            put(Double.class, Schema.Type.DOUBLE);
            put(Float.class, Schema.Type.FLOAT);
            put(String.class, Schema.Type.STRING);
            put(BytesWritable.class, Schema.Type.BYTES);

            // Note : Cascading field type for Array and Map is really a Tuple
            put(List.class, Schema.Type.ARRAY);
            put(Map.class, Schema.Type.MAP);

        }
    };

    public static Object[] parseTupleEntry(TupleEntry tupleEntry, Schema writerSchema) {
        if (!(writerSchema.getFields().size() == tupleEntry.size())) {
            throw new AvroRuntimeException("Arity mismatch between incoming tuple and schema");
        }

        return parseTuple(tupleEntry.getTuple(), writerSchema);
    }

    public static Object[] parseTuple(Tuple tuple, Schema writerSchema) {
        Object[] result = new Object[writerSchema.getFields().size()];

        List<Field> schemaFields = writerSchema.getFields();
        for (int i = 0; i < schemaFields.size(); i++) {
            Field field = schemaFields.get(i);

            // if (!fields.contains(new Fields(field.name()))) {
            // System.out.println(fields);
            // throw new RuntimeException("Tuple doesn't contain field: "+
            // field.name());
            // }
            Object obj = tuple.getObject(i);
            result[i] = toAvro(obj, field.schema());
        }
        
        return result;
    }

    protected static Object toAvro(Object obj, Schema schema) {
        switch (schema.getType()) {

            case ARRAY:
                return toAvroArray(obj, schema);

            case STRING:
                return obj.toString();
            case ENUM:
                return toAvroEnum(obj, schema);

            case FIXED:
                return toAvroFixed(obj, schema);
            case BYTES:
                return toAvroBytes(obj);

            case RECORD:
                Object[] objs;
                if (obj instanceof Tuple) {
                    objs = parseTuple((Tuple) obj, schema);
                } else {
                    objs = parseTupleEntry((TupleEntry) obj, schema);
                }

                Record record = new Record(schema);
                for (int i = 0; i < objs.length; i++) {
                    record.put(i, objs[i]);
                }
                
                return record;

            case MAP:
                return toAvroMap(obj, schema);

            case UNION:
                return toAvroUnion(obj, schema);

            case NULL:
            case BOOLEAN:
            case DOUBLE:
            case FLOAT:
            case INT:
            case LONG:
                return obj;

            default:
                throw new AvroRuntimeException("Can't convert from type " + schema.getType().toString());

        }
    }

    protected static Object toAvroEnum(Object obj, Schema schema) {
        return new GenericData.EnumSymbol(schema, obj.toString());
    }

    protected static Object toAvroFixed(Object obj, Schema schema) {
        BytesWritable bytes = (BytesWritable) obj;
        Fixed fixed = new Fixed(schema, Arrays.copyOfRange(bytes.getBytes(), 0, bytes.getLength()));
        return fixed;
    }

    @SuppressWarnings("unchecked")
    protected static Object toAvroMap(Object obj, Schema schema) {

        Map<String, Object> convertedMap = new HashMap<String, Object>();
        if (obj instanceof Tuple) {
            Type mapValueType = schema.getValueType().getType();
            Tuple tuple = (Tuple) obj;
            if (tuple.size() % 2 == 0) {
                for (int i = 0; i < tuple.size(); i = i + 2) {
                    if (tuple.getObject(i).getClass() != String.class) {
                        throw new AvroRuntimeException(
                                "Invalid map definition - the key should be a String - instead of "
                                        + tuple.getObject(i).getClass());
                    }
                    if (toAvroSchemaType(tuple.getObject(i + 1).getClass()) != mapValueType) {
                        throw new AvroRuntimeException(String.format("Found map value with %s instead of expected %s",
                                tuple.getObject(i + 1).getClass(), mapValueType));
                    }
                    convertedMap.put(tuple.getObject(i).toString(), toAvro(tuple.getObject(i + 1), schema.getValueType()));
                }
            } else {
                throw new AvroRuntimeException("Can't convert from an odd length tuple to a map");
            }
        } else {
            for (Map.Entry<String, Object> e : ((Map<String, Object>) obj).entrySet()) {
                convertedMap.put(e.getKey(), toAvro(e.getValue(), schema.getValueType()));
            }
        }
        return convertedMap;

    }

    protected static Object toAvroBytes(Object obj) {
        BytesWritable inBytes = (BytesWritable) obj;
        ByteBuffer buffer = ByteBuffer.wrap(Arrays.copyOfRange(inBytes.getBytes(), 0, inBytes.getLength()));
        return buffer;
    }

    protected static Object toAvroArray(Object obj, Schema schema) {
        if (obj instanceof Iterable) {
            Schema elementSchema = schema.getElementType();
            List<Object> array = new ArrayList<Object>();
            for (Object element : (Iterable<Object>) obj) {
                array.add(toAvro(element, elementSchema));
            }
            
            return new GenericData.Array(schema, array);
        } else
            throw new AvroRuntimeException("Can't convert from non-iterable to array");
    }

    protected static Object toAvroUnion(Object obj, Schema schema) {
        if (obj == null) {
            return obj;
        }

        List<Schema> types = schema.getTypes();
        if (types.size() < 1) {
            throw new AvroRuntimeException("Union in writer schema has no types");
        } else if (types.size() == 1) {
            return toAvro(obj, types.get(0));
        } else if (types.size() > 2) {
            throw new AvroRuntimeException("Unions may only consist of a concrete type and null in cascading.avro");
        } else if (!types.get(0).getType().equals(Type.NULL) && !types.get(1).getType().equals(Type.NULL)) {
            throw new AvroRuntimeException("Unions may only consist of a concrete type and null in cascading.avro");
        } else {
            Integer concreteIndex = (types.get(0).getType() == Type.NULL) ? 1 : 0;
            return toAvro(obj, types.get(concreteIndex));
        }
    }

    @SuppressWarnings("rawtypes")
    protected static Schema generateAvroSchemaFromTupleEntry(TupleEntry tupleEntry, String recordName,
            boolean isNullable) {
        Fields tupleFields = tupleEntry.getFields();
        List<Field> avroFields = new ArrayList<Field>();
        for (Comparable fieldName : tupleFields) {
            if (!(fieldName instanceof String)) {
                throw new AvroRuntimeException("Can't generate schema from non-string named fields");
            }
            Schema fieldSchema = generateAvroSchemaFromElement(tupleEntry.getObject(fieldName), (String) fieldName,
                    isNullable);
            avroFields.add(new Field((String) fieldName, fieldSchema, null, null));
        }

        Schema outputSchema = Schema.createRecord(recordName, "auto-generated by cascading.avro", null, false);
        outputSchema.setFields(avroFields);
        return outputSchema;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected static Schema generateAvroSchemaFromElement(Object element, String name, boolean isNullable) {
        if (element == null) {
            throw new AvroRuntimeException("Can't infer schema from null valued element");
        } else if (isNullable)
            return generateUnionSchema(element, name);
        else if (element instanceof TupleEntry)
            return generateAvroSchemaFromTupleEntry((TupleEntry) element, name, isNullable);
        else if (element instanceof Map)
            return generateAvroSchemaFromMap((Map<String, Object>) element, name);
        else if (element instanceof Iterable)
            return generateAvroSchemaFromIterable((Iterable) element, name);
        else if (element instanceof BytesWritable)
            return Schema.create(Schema.Type.BYTES);
        else if (element instanceof String)
            return Schema.create(Schema.Type.STRING);
        else if (element instanceof Double)
            return Schema.create(Schema.Type.DOUBLE);
        else if (element instanceof Float)
            return Schema.create(Schema.Type.FLOAT);
        else if (element instanceof Integer)
            return Schema.create(Schema.Type.INT);
        else if (element instanceof Long)
            return Schema.create(Schema.Type.LONG);
        else if (element instanceof Boolean)
            return Schema.create(Schema.Type.BOOLEAN);
        else
            throw new AvroRuntimeException("Can't create schema from type " + element.getClass());

    }

    private static Schema generateAvroSchemaFromIterable(Iterable element, String name) {
        Iterator<Object> iterator = element.iterator();
        if (!iterator.hasNext()) {
            throw new AvroRuntimeException("Can't infer list schema from empty iterable");
        } else {
            Schema itemSchema = generateAvroSchemaFromElement(iterator.next(), name + "ArrayElement", false);
            Schema arraySchema = Schema.createArray(itemSchema);
            return arraySchema;
        }

    }

    private static Schema generateAvroSchemaFromMap(Map<String, Object> element, String name) {
        if (element.isEmpty()) {
            throw new AvroRuntimeException("Can't infer map schema from empty map");
        } else {
            Iterator<Object> iterator = element.values().iterator();
            Schema valueSchema = generateAvroSchemaFromElement(iterator.next(), name + "MapValue", false);
            Schema mapSchema = Schema.createMap(valueSchema);
            return mapSchema;
        }

    }

    private static Schema generateUnionSchema(Object element, String name) {
        List<Schema> types = new ArrayList<Schema>();
        types.add(Schema.create(Schema.Type.NULL));
        types.add(generateAvroSchemaFromElement(element, name, false));
        Schema unionSchema = Schema.createUnion(types);
        return unionSchema;
    }

    public static Schema generateAvroSchemaFromFieldsAndTypes(String recordName, Fields schemeFields,
            Class<?>[] schemeTypes) {
        if (schemeFields.size() == 0) {
            throw new IllegalArgumentException("There must be at least one field");
        }

        int schemeTypesSize = 0;
        for (int i = 0; i < schemeTypes.length; i++, schemeTypesSize++) {
            if ((schemeTypes[i] == List.class) || (schemeTypes[i] == Map.class)) {
                i++;
            }
        }

        if (schemeTypesSize != schemeFields.size()) {
            throw new IllegalArgumentException("You must have a schemeType for every field");
        }

        for (int i = 0; i < schemeTypes.length; i++) {
            if ((schemeTypes[i] == List.class) || (schemeTypes[i] == Map.class)) {
                ++i;
                if (!isPrimitiveType(schemeTypes[i])) {
                    throw new IllegalArgumentException("Only primitive types are allowed for an Array or Map");
                }
            }
        }

        return generateSchema(recordName, schemeFields, schemeTypes, 0);
    }

    public static void addToTuple(Tuple t, byte[] bytes) {
        t.add(new BytesWritable(bytes));
    }

    @SuppressWarnings("rawtypes")
    public static void addToTuple(Tuple t, Enum e) {
        t.add(e.toString());
    }

    public static void addToTuple(Tuple t, List<?> list) {
        Tuple listTuple = new Tuple();
        for (Object item : list) {
            listTuple.add(item);
        }

        t.add(listTuple);
    }

    public static void addToTuple(Tuple t, Map<String, ?> map) {
        Tuple mapTuple = new Tuple();
        for (String key : map.keySet()) {
            mapTuple.add(key);
            mapTuple.add(map.get(key));
        }

        t.add(mapTuple);
    }

    private static boolean isPrimitiveType(Class<?> arrayType) {
        // only primitive types are allowed for arrays

        return (arrayType == Boolean.class || arrayType == Integer.class || arrayType == Long.class
                || arrayType == Float.class || arrayType == Double.class || arrayType == String.class || arrayType == BytesWritable.class);
    }

    private static Schema generateSchema(String recordName, Fields schemeFields, Class<?>[] schemeTypes, int depth) {
        // Create a 'record' that is made up of fields.
        // Since we support arrays and maps that means we can have nested
        // records

        List<Schema.Field> fields = new ArrayList<Schema.Field>();
        for (int typeIndex = 0, fieldIndex = 0; typeIndex < schemeTypes.length; typeIndex++, fieldIndex++) {
            String fieldName = schemeFields.get(fieldIndex).toString();
            Class<?>[] subSchemeTypes = new Class[2]; // at most 2, since we
                                                      // only allow primitive
                                                      // types for arrays and
                                                      // maps
            subSchemeTypes[0] = schemeTypes[typeIndex];
            if ((schemeTypes[typeIndex] == List.class) || (schemeTypes[typeIndex] == Map.class)) {
                typeIndex++;
                subSchemeTypes[1] = schemeTypes[typeIndex];
            }

            final Schema schema = createAvroSchema(recordName, schemeFields, subSchemeTypes, depth + 1);
            final Schema nullSchema = Schema.create(Schema.Type.NULL);
            List<Schema> schemas = new LinkedList<Schema>() {
                {
                    add(nullSchema);
                    add(schema);
                }
            };

            fields.add(new Schema.Field(fieldName, Schema.createUnion(schemas), "", null));
        }

        // Avro doesn't like anonymous records - so create a named one.
        if (depth > 0) {
            recordName = recordName + depth;
        }

        Schema schema = Schema.createRecord(recordName, "auto generated", "", false);
        schema.setFields(fields);
        return schema;
    }

    private static Schema createAvroSchema(String recordName, Fields schemeFields, Class<?>[] fieldTypes, int depth) {
        Schema.Type avroType = toAvroSchemaType(fieldTypes[0]);

        int remainingFields = schemeFields.size() - 1;
        if (avroType == Schema.Type.ARRAY) {
            Schema schema;
            if (remainingFields == 0) {
                schema = Schema.createArray(Schema.create(toAvroSchemaType(fieldTypes[1])));
            } else {
                Class<?> arrayTypes[] = { fieldTypes[1] };
                schema = Schema.createArray(createAvroSchema(recordName,
                        Fields.offsetSelector(schemeFields.size() - 1, 1), arrayTypes, depth + 1));
            }
            return schema;
        } else if (avroType == Schema.Type.MAP) {
            Schema schema;
            if (remainingFields == 0) {
                schema = Schema.createMap(Schema.create(toAvroSchemaType(fieldTypes[1])));
            } else {
                Class<?> mapTypes[] = { fieldTypes[1] };
                schema = Schema.createMap(createAvroSchema(recordName,
                        Fields.offsetSelector(schemeFields.size() - 1, 1), mapTypes, depth + 1));
            }
            return schema;
        } else if (avroType == Schema.Type.RECORD) {
            return generateSchema(recordName, Fields.offsetSelector(schemeFields.size() - 1, 1), fieldTypes, depth + 1);
        } else if (avroType == Schema.Type.ENUM) {
            Class<?> clazz = fieldTypes[0];
            Object[] names = clazz.getEnumConstants();
            List<String> enumNames = new ArrayList<String>(names.length);
            for (Object name : names) {
                enumNames.add(name.toString());
            }

            return Schema.createEnum(fieldTypes[0].getName(), null, null, enumNames);
        } else {
            return Schema.create(avroType);
        }
    }

    private static Schema.Type toAvroSchemaType(Class<?> clazz) {
        if (TYPE_MAP.containsKey(clazz)) {
            return TYPE_MAP.get(clazz);
        } else if (clazz.isEnum()) {
            return Schema.Type.ENUM;
        } else {
            throw new UnsupportedOperationException("The class type " + clazz + " is currently unsupported");
        }
    }
}
