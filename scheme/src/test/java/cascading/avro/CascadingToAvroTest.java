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

import cascading.avro.conversion.CascadingToAvro;
import cascading.avro.generated.Test5;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Fixed;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificFixed;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CascadingToAvroTest {

    TupleEntry tupleEntry = null;
    Schema schema = null;
    private static final int recordUnpackDepth = -1;
    private CascadingToAvro cascadingToAvro = new CascadingToAvro();

    @Before
    public void setUp() throws Exception {
        schema = new Schema.Parser().parse(getClass().getResourceAsStream("test5.avsc"));
        Fields fields = new Fields();
        for (Field avroField : schema.getFields())
            fields = fields.append(new Fields(avroField.name()));

        Tuple tuple = Tuple.size(11);
        tuple.set(0, false);
        tuple.set(1, 10);
        tuple.set(2, 5L);
        tuple.set(3, 0.6f);
        tuple.set(4, 1.01);
        tuple.set(5, "This is my string");
        byte[] buffer_value = {0, 1, 2, 3, 0, 0, 0};
        BytesWritable bytesWritableForFixed = new BytesWritable(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13,
            14, 15, 16});
        tuple.set(6, new BytesWritable(buffer_value));
        tuple.set(7, bytesWritableForFixed);
        List<Integer> array = new ArrayList<Integer>();
        array.add(0);
        array.add(1);
        tuple.set(8, array);
        Map<String, Integer> myMap = new HashMap<String, Integer>();
        myMap.put("one", 1);
        myMap.put("two", 2);
        tuple.set(9, myMap);
        tuple.set(10, 5);
        tupleEntry = new TupleEntry(fields, tuple);

    }

    @Test
    public void testSimpleUnion() {
        TupleEntry tupleEntryCopy = new TupleEntry(tupleEntry);
        tupleEntry.setObject("aUnion", tupleEntryCopy);
        IndexedRecord record = this.cascadingToAvro.convertRecord(tupleEntry, schema, recordUnpackDepth);
        IndexedRecord aUnion = (IndexedRecord) record.get(10);
        for (int i = 0; i < 10; i++) {
            assertEquals(record.get(i), aUnion.get(i));
        }
    }

    @Test
    public void testAmbiguousUnion() {
        Schema schema = SchemaBuilder
            .record("AmbiguousUnion").namespace("cascading.avro")
            .fields()
            .name("aUnion").type().unionOf()
            .array().items().intType().and()
            .map().values().intType()
            .endUnion().noDefault()
            .endRecord();
        // A CharSequence first element, and an even number of elements, so should result in map
        TupleEntry tupleEntryMap = new TupleEntry(new Fields("aUnion"), new Tuple(new Object[]{new Tuple("foo", 2, "bar", 4)}));
        IndexedRecord recordMap = cascadingToAvro.convertRecord(tupleEntryMap, schema, recordUnpackDepth);

        // Am even number of elements, but a non-CharSequence first element, so should result in array
        TupleEntry tupleEntryArray = new TupleEntry(new Fields("aUnion"), new Tuple(new Object[]{new Tuple(1, 2, 3, 4)}));
        IndexedRecord recordArray = cascadingToAvro.convertRecord(tupleEntryArray, schema, recordUnpackDepth);

        // We have a CharSequence first element, an even number of elements, but a non-CharSequence element in a key position
        // Our heuristic fails, but the conversion should not succeed.
        TupleEntry tupleEntryFail1 = new TupleEntry(new Fields("aUnion"), new Tuple(new Object[]{new Tuple("foo", 2, 3, 4)}));

        // Our heuristic fails and we convert to array but the conversion should not succeed
        TupleEntry tupleEntryFail2 = new TupleEntry(new Fields("aUnion"), new Tuple(new Object[]{new Tuple(1, 2, "foo", 4)}));

        assertEquals(ImmutableMap.builder().put("foo", 2).put("bar", 4).build(), recordMap.get(0));
        assertEquals(Arrays.asList(1, 2, 3, 4), recordArray.get(0));
        try {
            cascadingToAvro.convertRecord(tupleEntryFail1, schema, recordUnpackDepth);
        }
        catch (AvroRuntimeException e1) {
            try {
                cascadingToAvro.convertRecord(tupleEntryFail2, schema, recordUnpackDepth);
            }
            catch (AvroRuntimeException e2) {
                return;
            }
        }
        fail();
    }

    @Test
    public void testFixedUnion(){
        Schema fixedSchema = SchemaBuilder.builder()
            .fixed("UUID")
            .size(16);

        Schema unionSchema = SchemaBuilder.builder()
            .unionOf()
            .type(fixedSchema)
            .and()
            .nullType()
            .endUnion();

        Fixed fixed = new Fixed(fixedSchema);

        fixed.bytes(new byte[]{
            0, 2, 1, 4, 4, 3, 2, 2,
            0, 2, 1, 9, 4, 3, 8, 7
        });

        GenericFixed fixedOut = (GenericFixed) cascadingToAvro.convertUnion(fixed, unionSchema, recordUnpackDepth);
        assertEquals(fixed, fixedOut);
    }

    @Test
    public void testArrayUnion() {
        Schema unionSchema = new Schema.Parser()
            .parse("[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}]");
        Schema schema = SchemaBuilder
            .record("WithUnion").namespace("cascading.avro")
            .fields().name("union").type(unionSchema).noDefault()
            .endRecord();
        tupleEntry = new TupleEntry(new Fields("union"), new Tuple(Arrays.asList("en_GB")));
        IndexedRecord record = this.cascadingToAvro.convertRecord(tupleEntry, schema, recordUnpackDepth);
        assertEquals(record.get(0), Arrays.asList("en_GB"));
    }

    @Test
    public void testBytesUnion() {
        Schema unionSchema = new Schema.Parser()
            .parse("[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}]");
        Schema schema = SchemaBuilder
            .record("WithUnion").namespace("cascading.avro")
            .fields().name("union").type(unionSchema).noDefault()
            .endRecord();
        tupleEntry = new TupleEntry(new Fields("union"), new Tuple(Arrays.asList("en_GB")));
        IndexedRecord record = this.cascadingToAvro.convertRecord(tupleEntry, schema, recordUnpackDepth);
        assertEquals(record.get(0), Arrays.asList("en_GB"));
    }

    @Test
    public void testToArrayPrimitive() {
        Schema fieldSchema = schema.getField("aList").schema();
        List<Integer> outList = (GenericData.Array<Integer>) this.cascadingToAvro.convert(tupleEntry.getObject(8),
            fieldSchema,
            recordUnpackDepth);

        assertThat(outList.get(0), is(0));
        assertThat(outList.get(1), is(1));
    }

    @Test
    public void testFromTupleToArray() {
        Schema fieldSchema = schema.getField("aList").schema();
        Tuple tuple = new Tuple();
        tuple.add(0);
        tuple.add(1);
        List<Integer> outList = (GenericData.Array<Integer>) this.cascadingToAvro.convert(tuple, fieldSchema, recordUnpackDepth);

        assertThat(outList.get(0), is(0));
        assertThat(outList.get(1), is(1));
    }

    @Test
    public void testFromTupleToMap() {
        Schema fieldSchema = schema.getField("aMap").schema();
        Tuple tuple = new Tuple();
        tuple.add("one");
        tuple.add(1);
        tuple.add("two");
        tuple.add(2);
        Map<String, Integer> outMap = (Map<String, Integer>) this.cascadingToAvro.convert(tuple, fieldSchema, recordUnpackDepth);

        assertThat(outMap.get("one"), is(1));
        assertThat(outMap.get("two"), is(2));
    }

    @Test
    public void testFromArrayNested() {
        Schema innerSchema = Schema.createArray(Schema.create(Schema.Type.INT));
        Schema outerSchema = Schema.createArray(innerSchema);
        GenericArray<GenericArray<Integer>> array = new GenericData.Array<GenericArray<Integer>>(1, outerSchema);
        GenericArray<Integer> innerArray = new GenericData.Array<Integer>(1, innerSchema);
        innerArray.add(0);
        innerArray.add(1);
        array.add(innerArray);

        List<List<Integer>> outList = (List<List<Integer>>) this.cascadingToAvro.convert(array, outerSchema, recordUnpackDepth);

        assertThat(outList.get(0), is((List<Integer>) innerArray));
        assertThat(outList.get(0).get(0), is(0));
        assertThat(outList.get(0).get(1), is(1));
    }

    @Test
    public void testFromMapPrimitive() {
        Schema fieldSchema = schema.getField("aMap").schema();
        Map<String, Integer> outMap = (Map<String, Integer>) this.cascadingToAvro.convert(tupleEntry.getObject(9), fieldSchema,
            recordUnpackDepth);

        assertThat(outMap.get("one"), is(1));
        assertThat(outMap.get("two"), is(2));
    }

    @Test
    public void testFromMapNested() {
        Schema innerSchema = Schema.createMap(Schema.create(Schema.Type.LONG));
        Schema outerSchema = Schema.createMap(innerSchema);
        Map<String, Long> innerMap = new HashMap<String, Long>();
        innerMap.put("one", 1L);
        innerMap.put("two", 2L);
        Map<String, Map<String, Long>> outerMap = new HashMap<String, Map<String, Long>>();
        outerMap.put("map1", innerMap);
        Map<String, Map<String, Long>> outMap = (Map<String, Map<String, Long>>)
            this.cascadingToAvro.convert(outerMap, outerSchema, recordUnpackDepth);

        assertThat(outMap.get("map1"), is(innerMap));
        assertThat(outMap.get("map1").get("two"), is(2L));
        assertThat(outMap.get("map1").get("one"), is(1L));
    }

    @Test
    public void testFromBytes() {
        Schema fieldSchema = schema.getField("aBytes").schema();
        byte[] buffer_value = {0, 1, 2, 3, 0, 0, 0};
        ByteBuffer result = ByteBuffer.wrap(buffer_value);

        ByteBuffer outBytes = (ByteBuffer) this.cascadingToAvro.convert(tupleEntry.getObject("aBytes"), fieldSchema, recordUnpackDepth);

        assertThat(outBytes, is(result));
    }

    @Test
    public void testFromFixed() {
        Schema fieldSchema = schema.getField("aFixed").schema();
        Fixed result = new Fixed(fieldSchema, new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});

        Fixed outFixed = (Fixed) this.cascadingToAvro.convert(tupleEntry.getObject("aFixed"), fieldSchema, recordUnpackDepth);

        assertThat(outFixed, is(result));
    }

    @Test
    public void testFromUnion() {
        Schema fieldSchema = schema.getField("aUnion").schema();
        Integer outInt = (Integer) this.cascadingToAvro.convert(tupleEntry.getObject(10), fieldSchema, recordUnpackDepth);

        assertThat(outInt, is(5));
    }

    @Test
    public void testParseTupleEntry() {
        byte[] buffer_value = {0, 1, 2, 3, 0, 0, 0};
        ByteBuffer buffer = ByteBuffer.wrap(buffer_value);
        Fixed fixed = new Fixed(schema.getField("aFixed").schema(), new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
            13, 14, 15, 16});

        Test5 record = (Test5) this.cascadingToAvro.convertRecord(tupleEntry, schema, recordUnpackDepth);

        assertThat((Boolean) record.get(0), is(false));
        assertThat((Integer) record.get(1), is(10));
        assertThat((Long) record.get(2), is(5L));
        assertThat((Float) record.get(3), is(0.6f));
        assertThat((Double) record.get(4), is(1.01));
        assertThat((String) record.get(5), is("This is my string"));
        assertThat((ByteBuffer) record.get(6), is(buffer));
        assertThat((Fixed) record.get(7), is(fixed));
        List<Integer> outList = (List<Integer>) record.get(8);
        assertThat(outList.get(0), is(0));
        assertThat(outList.get(1), is(1));
        Map<String, Integer> outMap = (Map<String, Integer>) record.get(9);
        assertThat(outMap.get("one"), is(1));
        assertThat(outMap.get("two"), is(2));
        assertThat((Integer) record.get(10), is(5));
    }

    @Test
    public void testArraySchema() {
        List<Integer> array = new ArrayList<Integer>();
        array.add(0);
        Schema arraySchema = AvroSchemata.generateAvroSchemaFromElement(array, "my array", false);
        Schema expected = Schema.createArray(Schema.create(Type.INT));
        assertThat(arraySchema, is(expected));
    }

    @Test
    public void testMapSchema() {
        Map<String, Double> map = new HashMap<String, Double>();
        map.put("one", 1.01);
        Schema mapSchema = AvroSchemata.generateAvroSchemaFromElement(map, "my map", false);
        Schema expected = Schema.createMap(Schema.create(Type.DOUBLE));
        assertThat(mapSchema, is(expected));
    }

    @Test
    public void testUnionSchema() {
        Long l = 5L;
        Schema unionSchema = AvroSchemata.generateAvroSchemaFromElement(l, "my long", true);
        List<Schema> types = new ArrayList<Schema>();
        types.add(Schema.create(Schema.Type.NULL));
        types.add(Schema.create(Schema.Type.LONG));
        Schema expected = Schema.createUnion(types);
        assertThat(unionSchema, is(expected));
    }

    @Test
    public void testGenerateSchemaFromTupleEntry() {
        Schema expected = new Schema.Parser()
            .parse("{\"type\":\"record\",\"name\":\"CascadingRecord\",\"doc\":\"auto-generated by cascading.avro\",\"fields\":[{\"name\":\"aBoolean\",\"type\":\"boolean\"},{\"name\":\"anInt\",\"type\":\"int\"},{\"name\":\"aLong\",\"type\":\"long\"},{\"name\":\"aFloat\",\"type\":\"float\"},{\"name\":\"aDouble\",\"type\":\"double\"},{\"name\":\"aString\",\"type\":\"string\"},{\"name\":\"aBytes\",\"type\":\"bytes\"},{\"name\":\"aFixed\",\"type\":\"bytes\"},{\"name\":\"aList\",\"type\":{\"type\":\"array\",\"items\":\"int\"}},{\"name\":\"aMap\",\"type\":{\"type\":\"map\",\"values\":\"int\"}},{\"name\":\"aUnion\",\"type\":\"int\"}]}");
        Schema outSchema = AvroSchemata.generateAvroSchemaFromTupleEntry(tupleEntry, "CascadingRecord", false);
        assertThat(outSchema, is(expected));
    }

    @Test
    public void testToAvroFixedUsesValidRangeOfBytesWritable() {
        Schema fieldSchema = schema.getField("aFixed").schema();
        BytesWritable bytes = new BytesWritable();
        byte[] old_buffer_value = {0, 1, 2, 3};
        bytes.set(old_buffer_value, 0, old_buffer_value.length);

        byte[] buffer_value = {4, 5, 6};
        bytes.set(buffer_value, 0, buffer_value.length);
        byte[] outBytes = ((Fixed) this.cascadingToAvro.convertFixed(bytes, fieldSchema)).bytes();

        assertThat(outBytes, is(buffer_value));
    }

    @Test
    public void testToAvroBytesUsesValidRangeOfBytesWritable() {
        Schema fieldSchema = schema.getField("aBytes").schema();
        BytesWritable bytes = (BytesWritable) tupleEntry.getObject("aBytes");
        byte[] old_buffer_value = {0, 1, 2, 3};
        bytes.set(old_buffer_value, 0, old_buffer_value.length);

        byte[] buffer_value = {4, 5, 6};
        ByteBuffer result = ByteBuffer.wrap(buffer_value);
        bytes.set(buffer_value, 0, buffer_value.length);
        ByteBuffer outBytes = (ByteBuffer) this.cascadingToAvro.convert(bytes, fieldSchema, recordUnpackDepth);

        assertThat(outBytes, is(result));
    }
}
