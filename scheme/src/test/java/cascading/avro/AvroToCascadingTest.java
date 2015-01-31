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

import cascading.avro.conversion.AvroToCascading;
import cascading.avro.generated.TreeNode;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Fixed;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static cascading.avro.conversion.TypeMappings.asMap;
import static cascading.avro.conversion.TypeMappings.asMapDeep;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;

public class AvroToCascadingTest {
    Record record = null;
    private static final int recordUnpackDepth = -1;
    private AvroToCascading avroToCascading = new AvroToCascading();

    @Before
    public void setUp() throws Exception {
        Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("test5.avsc"));
        record = new Record(schema);
        record.put(0, false);
        record.put(1, 10);
        record.put(2, 5L);
        record.put(3, 0.6f);
        record.put(4, 1.01);
        record.put(5, "This is my string");
        byte[] buffer_value = {0, 1, 2, 3, 0, 0, 0};
        ByteBuffer buffer = ByteBuffer.wrap(buffer_value);
        record.put(6, buffer);
        Fixed fixed = new Fixed(schema.getField("aFixed").schema(), buffer_value);
        record.put(7, fixed);
        Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.INT));
        GenericArray<Integer> array = new GenericData.Array<Integer>(1, arraySchema);
        array.add(0);
        array.add(1);
        record.put(8, array);
        Map<Utf8, Integer> myMap = new HashMap<Utf8, Integer>();
        myMap.put(new Utf8("one"), 1);
        myMap.put(new Utf8("two"), 2);
        record.put(9, myMap);
        record.put(10, 5);
    }

    @Test
    public void testFromArrayPrimitive() {
        Schema fieldSchema = record.getSchema().getField("aList").schema();
        Tuple outList = (Tuple) this.avroToCascading.convert(record.get(8), fieldSchema, recordUnpackDepth);

        assertEquals(outList.getInteger(0), 0);
        assertEquals(outList.getInteger(1), 1);
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

        Tuple outList = (Tuple) this.avroToCascading.convert(array, outerSchema, recordUnpackDepth);

        assertEquals(outList.getObject(0), new Tuple(0, 1));
    }

    @Test
    public void testFromMapPrimitive() {
        Schema fieldSchema = record.getSchema().getField("aMap").schema();
        Map<String, Integer> outMap = asMap(this.avroToCascading.convert(record.get(9), fieldSchema, recordUnpackDepth));

        assertThat(outMap.get("one"), is(1));
        assertThat(outMap.get("two"), is(2));
    }

    @Test
    public void testFromMapNested() {
        Schema innerSchema = Schema.createMap(Schema.create(Schema.Type.LONG));
        Schema outerSchema = Schema.createMap(innerSchema);
        Map<Utf8, Long> innerMap = new HashMap<Utf8, Long>();
        innerMap.put(new Utf8("one"), 1L);
        innerMap.put(new Utf8("two"), 2L);
        Map<Utf8, Map<Utf8, Long>> outerMap = new HashMap<Utf8, Map<Utf8, Long>>();
        outerMap.put(new Utf8("map1"), innerMap);
        Map<String, Map<String, Long>> outMap = asMapDeep(this.avroToCascading.convert(outerMap,
            outerSchema, recordUnpackDepth));

        assertThat(outMap.get("map1").get("two"), is(2L));
        assertThat(outMap.get("map1").get("one"), is(1L));
    }

    @Test
    public void testFromBytes() {
        Schema fieldSchema = record.getSchema().getField("aBytes").schema();
        byte[] buffer_value = {0, 1, 2, 3, 0, 0, 0};
        BytesWritable result = new BytesWritable(buffer_value);

        BytesWritable outBytes = (BytesWritable) this.avroToCascading.convert(record.get("aBytes"), fieldSchema, recordUnpackDepth);

        assertThat(outBytes, is(result));
    }

    @Test
    public void testFromFixed() {
        Schema fieldSchema = record.getSchema().getField("aFixed").schema();
        byte[] buffer_value = {0, 1, 2, 3, 0, 0, 0};
        BytesWritable result = new BytesWritable(buffer_value);

        BytesWritable outBytes = (BytesWritable) this.avroToCascading.convert(record.get("aFixed"), fieldSchema, recordUnpackDepth);

        assertThat(outBytes, is(result));
    }

    @Test
    public void testFromUnion() {
        Schema fieldSchema = record.getSchema().getField("aUnion").schema();
        Integer outInt = (Integer) this.avroToCascading.convert(record.get(10), fieldSchema, recordUnpackDepth);

        assertThat(outInt, is(5));
    }

    @Test
    public void testConversionDepth() {
        // Build a tree with depth = 3
        TreeNode deepLeafNode = new TreeNode("welcome", 4, new ArrayList<TreeNode>());
        TreeNode subtree = new TreeNode("and", 3, Arrays.asList(deepLeafNode));
        TreeNode leafNode = new TreeNode("this", 2, new ArrayList<TreeNode>());
        TreeNode tree = new TreeNode("Hello", 1, Arrays.asList(leafNode, subtree));

        // if we set toDepth = 2 we should find an unconverted leaf TreeNode with label welcome
        TupleEntry shallowTupleEntry = this.avroToCascading.convertRecord(tree, tree.getSchema(), 2);
        Tuple helloChildren = ((Tuple) shallowTupleEntry.getObject("children"));
        Tuple andChildren = (Tuple) ((TupleEntry) helloChildren.getObject(1)).getObject("children");
        TupleEntry thisTupleEntry = (TupleEntry) helloChildren.getObject(0);
        TreeNode welcomeNode = (TreeNode) andChildren.getObject(0);
        assertEquals(thisTupleEntry.getObject("label"), "this");
        assertEquals(welcomeNode, deepLeafNode);

        // whereas if we set toDepth = -1 (unlimited) we should get TupleEntries for records all the way down
        TupleEntry deepTupleEntry = this.avroToCascading.convertRecord(tree, tree.getSchema(), -1);
        helloChildren = ((Tuple) deepTupleEntry.getObject("children"));
        andChildren = (Tuple) ((TupleEntry) helloChildren.getObject(1)).getObject("children");
        TupleEntry welcomeTupleEntry = (TupleEntry) andChildren.getObject(0);
        assertEquals(welcomeTupleEntry.getObject("label"), "welcome");
    }

    @Test
    public void testParseRecord() {
        byte[] buffer_value = {0, 1, 2, 3, 0, 0, 0};
        BytesWritable bwritable = new BytesWritable(buffer_value);

        Tuple tuple = this.avroToCascading.convertRecord(record, record.getSchema(), recordUnpackDepth)
            .getTuple();

        assertThat(tuple.size(), is(11));
        assertThat(tuple.getBoolean(0), is(false));
        assertThat(tuple.getInteger(1), is(10));
        assertThat(tuple.getLong(2), is(5L));
        assertThat(tuple.getFloat(3), is(0.6f));
        assertThat(tuple.getDouble(4), is(1.01));
        assertThat(tuple.getString(5), is("This is my string"));
        assertThat((BytesWritable) tuple.getObject(6), is(bwritable));
        assertThat((BytesWritable) tuple.getObject(7), is(bwritable));
        assertEquals(tuple.getObject(8), new Tuple(0, 1));
        Map<String, Integer> outMap = asMap(tuple.getObject(9));
        assertThat(outMap.get("one"), is(1));
        assertThat(outMap.get("two"), is(2));
        assertThat(tuple.getInteger(10), is(5));
    }

    @Test
    public void testNullFieldValue() {
        String schemaStr = "{" +
            "\"type\":\"record\", " +
            "\"name\": \"nulltest\"," +
            "\"fields\":[" +
            "	{\"name\":\"afield\", \"type\":\"string\"}," +
            "   {\"name\":\"aMap\", \"type\":{\"type\":\"map\", \"values\":\"string\"}}," +
            "   {\"name\":\"bMap\", \"type\":{\"type\":\"map\", \"values\":\"string\"}}]}";

        Schema schema = new Schema.Parser().parse(schemaStr);
        Record rec = new Record(schema);
        rec.put(0, null);
        Map<Utf8, String> aMap = new HashMap<Utf8, String>();
        aMap.put(new Utf8("one"), "foo");
        aMap.put(new Utf8("two"), null);
        rec.put(1, aMap);

        Map<Utf8, String> bMap = null;
        rec.put(2, bMap);

        Tuple tuple = this.avroToCascading.convertRecord(rec, rec.getSchema(), recordUnpackDepth).getTuple();

        assertThat(tuple.getObject(0), nullValue());

        Map<String, String> outMap = asMap(tuple.getObject(1));
        assertThat(outMap.get("one"), is("foo"));
        assertThat(outMap.get("two"), nullValue());

        assertThat(tuple.getObject(2), nullValue());
    }
}
