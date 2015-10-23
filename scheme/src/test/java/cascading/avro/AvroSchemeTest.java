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

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.*;
import junit.framework.Assert;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.util.*;

/**
 * Class AvroSchemeTest
 */
public class AvroSchemeTest extends Assert {

  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();

  @Test
  public void testRoundTrip() throws Exception {
    final Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream(
        "test1.avsc"));

    final Fields fields = new Fields("aBoolean", "anInt", "aLong",
                                     "aDouble", "aFloat", "aBytes", "aFixed", "aNull", "aString",
                                     "aList", "aMap", "aUnion");

    String in = tempDir.getRoot().toString() + "/testRoundTrip/in";
    String out = tempDir.getRoot().toString() + "/testRoundTrip/out";
    Tap lfsSource = new Lfs(new AvroScheme(schema), in, SinkMode.REPLACE);
    TupleEntryCollector write = lfsSource.openForWrite(new HadoopFlowProcess());

    List<Integer> aList = new ArrayList<Integer>();
    Map<String, Integer> aMap = new HashMap<String, Integer>();
    aMap.put("one", 1);
    aMap.put("two", 2);

    aList.add(0);
    aList.add(1);
    BytesWritable bytesWritable = new BytesWritable(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
    BytesWritable bytesWritable2 = new BytesWritable(new byte[]{1,
        2, 3});
    Tuple tuple = new Tuple(false, 1, 2L, 3.0, 4.0F, bytesWritable2,
                            bytesWritable, null, "test-string", aList, aMap, 5);
    write.add(new TupleEntry(fields, tuple));
    write.add(new TupleEntry(fields, new Tuple(false, 1, 2L,
                                               3.0, 4.0F, new BytesWritable(new byte[0]), new BytesWritable(
        new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4,
            5, 6}), null, "other string", aList, aMap, null)));
    write.close();


    Pipe writePipe = new Pipe("tuples to avro");

    Tap avroSink = new Lfs(new AvroScheme(schema), out);
    Flow flow = new HadoopFlowConnector().connect(lfsSource, avroSink, writePipe);
    flow.complete();

    // Now read it back in, and verify that the data/types match up.
    Tap avroSource = new Lfs(new AvroScheme(schema), out);


    TupleEntryIterator iterator = avroSource.openForRead(new HadoopFlowProcess());

    assertTrue(iterator.hasNext());
    final TupleEntry readEntry1 = iterator.next();

    assertEquals(false, readEntry1.getBoolean(0));
    assertEquals(1, readEntry1.getInteger(1));
    assertEquals(2L, readEntry1.getLong(2));
    assertEquals(3.0, readEntry1.getDouble(3), 0.01);
    assertEquals(4.0F, readEntry1.getFloat(4), 0.01);
    assertEquals(bytesWritable2, readEntry1.getObject(5));
    assertEquals(bytesWritable, readEntry1.getObject(6));
    assertEquals("test-string", readEntry1.getString(8));
    assertEquals("0", ((List) readEntry1.getObject(9)).get(0)
        .toString());
    assertEquals(1,
                 ((Map) readEntry1.getObject(10)).get("one"));
    assertTrue(iterator.hasNext());
    final TupleEntry readEntry2 = iterator.next();

    assertNull(readEntry2.getObject("aUnion"));
  }

  @Test
  public void notUnpackedTest() throws Exception {
    final Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream(
        "test2.avsc"));
    String in = tempDir.getRoot().toString() + "/recordtest/in";
    String out = tempDir.getRoot().toString() + "/recordtest/out";
    Tap lfsSource = new Lfs(new PackedAvroScheme<Record>(schema), in, SinkMode.REPLACE);
    TupleEntryCollector write = lfsSource.openForWrite(new HadoopFlowProcess());
    Tuple tuple = Tuple.size(1);
    Record record = new Record(schema);
    String innerSchema = "{\"type\":\"record\", \"name\":\"nested\", \"namespace\": \"cascading.avro\", \"fields\": [{\"name\":\"anInnerField1\", \"type\":\"int\"}, {\"name\":\"anInnerField2\", \"type\":\"string\"} ] }";
    Record inner = new Record(new Schema.Parser().parse(innerSchema));
    inner.put(0, 0);
    inner.put(1, "the string");
    record.put(0, inner);
    record.put(1, "outer string");
    tuple.set(0, record);
    TupleEntry te = new TupleEntry(new Fields("test2"), tuple);
    write.add(te);
    write.close();


    Pipe writePipe = new Pipe("tuples to avro");

    Tap avroSink = new Lfs(new PackedAvroScheme<Record>(schema), out);
    Flow flow = new HadoopFlowConnector().connect(lfsSource, avroSink, writePipe);
    flow.complete();

    // Now read it back in, and verify that the data/types match up.
    Tap avroSource = new Lfs(new PackedAvroScheme<Record>(schema), out);


    TupleEntryIterator iterator = avroSource.openForRead(new HadoopFlowProcess());

    assertTrue(iterator.hasNext());
    final TupleEntry readEntry1 = iterator.next();

    assertTrue(readEntry1.getObject(0) instanceof IndexedRecord);
    assertEquals(((IndexedRecord) record.get(0)).get(0), ((IndexedRecord) ((IndexedRecord) readEntry1.getObject(0)).get(0)).get(0));
    assertEquals(new Utf8((String) ((IndexedRecord) record.get(0)).get(1)), ((IndexedRecord) ((IndexedRecord) readEntry1.getObject(0)).get(0)).get(1));
    assertEquals(new Utf8((String) record.get(1)), ((IndexedRecord) readEntry1.getObject(0)).get(1));

  }

  @Test
  public void tupleInsideTupleTest() throws Exception {
    final Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream(
        "test2.avsc"));
    String in = tempDir.getRoot().toString() + "/tupleInside/in";
    String out = tempDir.getRoot().toString() + "/tupleInside/out";
    Tap lfsSource = new Lfs(new AvroScheme(schema), in, SinkMode.REPLACE);
    TupleEntryCollector write = lfsSource.openForWrite(new HadoopFlowProcess());
    Fields fields = new Fields("innerRec", "outerField");
    Tuple tuple = Tuple.size(2);

    Tuple inner = Tuple.size(2);
    inner.set(0, 0);
    inner.set(1, "the string");
    TupleEntry record = new TupleEntry(new Fields("anInnerField1", "anInnerField2"), inner);
    tuple.set(0, record);
    tuple.set(1, "outer string");
    write.add(new TupleEntry(fields, tuple));
    write.close();


    Pipe writePipe = new Pipe("tuples to avro");

    Tap avroSink = new Lfs(new AvroScheme(schema), out);
    Flow flow = new HadoopFlowConnector().connect(lfsSource, avroSink, writePipe);
    flow.complete();

    // Now read it back in, and verify that the data/types match up.
    Tap avroSource = new Lfs(new AvroScheme(schema), out);


    TupleEntryIterator iterator = avroSource.openForRead(new HadoopFlowProcess());

    assertTrue(iterator.hasNext());
    final TupleEntry readEntry1 = iterator.next();

    assertTrue(readEntry1.getObject(0) instanceof Tuple);
    assertEquals(0, ((Tuple) readEntry1.getObject(0)).getInteger(0));
    assertEquals("the string", ((Tuple) readEntry1.getObject(0)).getString(1));
    assertEquals("outer string", readEntry1.getString(1));

  }

  @Test
  public void listOrMapInsideListTest() throws Exception {
    final Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream(
        "test4.avsc"));
    final AvroScheme scheme = new AvroScheme(schema);

    final Fields fields = new Fields("aListOfListOfInt", "aListOfMapToLong");

    final Lfs lfs = new Lfs(scheme, tempDir.getRoot().toString());
    HadoopFlowProcess writeProcess = new HadoopFlowProcess(new JobConf());
    final TupleEntryCollector collector = lfs.openForWrite(writeProcess);

    List<Map<String, Long>> aListOfMapToLong = new ArrayList<Map<String, Long>>();
    Map<String, Long> aMapToLong = new HashMap<String, Long>();
    aMapToLong.put("one", 1L);
    aMapToLong.put("two", 2L);
    aListOfMapToLong.add(aMapToLong);

    List<List<Integer>> aListOfListOfInt = new ArrayList<List<Integer>>();
    List<Integer> aListOfInt = new LinkedList<Integer>();
    aListOfInt.add(0);
    aListOfInt.add(1);
    aListOfListOfInt.add(aListOfInt);

    write(scheme, collector, new TupleEntry(fields, new Tuple(
        aListOfListOfInt, aListOfMapToLong)));
    collector.close();

    HadoopFlowProcess readProcess = new HadoopFlowProcess(new JobConf());
    final TupleEntryIterator iterator = lfs.openForRead(readProcess);
    assertTrue(iterator.hasNext());
    final TupleEntry readEntry1 = iterator.next();

    List<Integer> outListOfInt = (List) ((List) readEntry1
        .getObject("aListOfListOfInt")).get(0);
    Map<Utf8, Long> outMapToLong = (Map) ((List) readEntry1
        .getObject("aListOfMapToLong")).get(0);

    assertEquals(Integer.valueOf(0), outListOfInt.get(0));
    assertEquals(Integer.valueOf(1), outListOfInt.get(1));
    assertEquals(Long.valueOf(1L), outMapToLong.get("one"));
    assertEquals(Long.valueOf(2L), outMapToLong.get("two"));
    assertTrue(!iterator.hasNext());

  }

  @Test
  public void listOrMapInsideMapTest() throws Exception {
    final Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream(
        "test3.avsc"));
    final AvroScheme scheme = new AvroScheme(schema);

    final Fields fields = new Fields("aMapToListOfInt", "aMapToMapToLong");

    final Lfs lfs = new Lfs(scheme, tempDir.getRoot().toString());
    HadoopFlowProcess writeProcess = new HadoopFlowProcess(new JobConf());
    final TupleEntryCollector collector = lfs.openForWrite(writeProcess);

    Map<String, Map<String, Long>> aMapToMapToLong = new HashMap<String, Map<String, Long>>();
    Map<String, Long> aMapToLong = new HashMap<String, Long>();
    aMapToLong.put("one", 1L);
    aMapToLong.put("two", 2L);
    aMapToMapToLong.put("key", aMapToLong);

    Map<String, List<Integer>> aMapToListOfInt = new HashMap<String, List<Integer>>();
    List<Integer> aListOfInt = new LinkedList<Integer>();
    aListOfInt.add(0);
    aListOfInt.add(1);
    aMapToListOfInt.put("key", aListOfInt);

    write(scheme, collector, new TupleEntry(fields, new Tuple(
        aMapToListOfInt, aMapToMapToLong)));
    collector.close();

    HadoopFlowProcess readProcess = new HadoopFlowProcess(new JobConf());
    final TupleEntryIterator iterator = lfs.openForRead(readProcess);
    assertTrue(iterator.hasNext());
    final TupleEntry readEntry1 = iterator.next();

    List<Integer> outListOfInt = (List) ((Map) readEntry1
        .getObject("aMapToListOfInt")).get("key");
    Map<String, Long> outMapToLong = (Map) ((Map) readEntry1
        .getObject("aMapToMapToLong")).get("key");

    assertEquals(Integer.valueOf(0), outListOfInt.get(0));
    assertEquals(Integer.valueOf(1), outListOfInt.get(1));
    assertEquals(Long.valueOf(1L), outMapToLong.get("one"));
    assertEquals(Long.valueOf(2L), outMapToLong.get("two"));
    assertTrue(!iterator.hasNext());

  }

  private void write(AvroScheme scheme, TupleEntryCollector collector,
                     TupleEntry te) {
    collector.add(te.selectTuple(scheme.getSinkFields()));
  }

  @Test
  public void testSerialization() throws Exception {
    final Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream(
        "test1.avsc"));
    final AvroScheme expected = new AvroScheme(schema);

    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(bytes);
    oos.writeObject(expected);
    oos.close();

    final ObjectInputStream iis = new ObjectInputStream(
        new ByteArrayInputStream(bytes.toByteArray()));
    final AvroScheme actual = (AvroScheme) iis.readObject();

    assertEquals(expected, actual);
  }


  @Test
  public void testSchemeChecks() {

    try {
      new AvroScheme(new Fields("a", "b"), new Class[]{String.class, Long.class});
    } catch (Exception e) {
      fail("Exception should not be thrown - this is the valid case");
    }

    try {
      new AvroScheme(new Fields(), new Class[]{});
      fail("Exception should be thrown when scheme field is empty");
    } catch (Exception e) {
    }

    try {
      new AvroScheme(new Fields("a", "b", "c"), new Class[]{Integer.class});
      fail("Exception should be thrown as there are more fields defined than types");
    } catch (Exception e) {
    }

    try {
      new AvroScheme(new Fields("a"), new Class[]{Integer.class, String.class});
      fail("Exception should be thrown as there are more types defined than fields");
    } catch (Exception e) {
    }

    try {
      new AvroScheme(new Fields("array"), new Class[]{List.class, Long.class});
    } catch (Exception e) {
      fail("Exception shouldn't be thrown as array type is valid");
    }

    try {
      new AvroScheme(new Fields("array"), new Class[]{List.class, List.class});
      fail("Exception should be thrown as array type isn't a primitive");
    } catch (Exception e) {
    }

    try {
      new AvroScheme(new Fields("map"), new Class[]{Map.class, Long.class});
    } catch (Exception e) {
      fail("Exception shouldn't be thrown as map type is valid");
    }

    try {
      new AvroScheme(new Fields("map"), new Class[]{Map.class, List.class});
      fail("Exception should be thrown as map type isn't a primitive");
    } catch (Exception e) {
    }


  }


  @SuppressWarnings("serial")
  @Test
  public void testRoundTrip2() throws Exception {

    // Create a scheme that tests each of the supported types

    final Fields testFields = new Fields("integerField", "longField", "booleanField", "doubleField", "floatField",
                                         "stringField", "bytesField", "arrayOfLongsField", "mapOfStringsField", "enumField");
    final Class<?>[] schemeTypes = {Integer.class, Long.class, Boolean.class, Double.class, Float.class,
        String.class, BytesWritable.class, List.class, Long.class, Map.class, String.class, TestEnum.class};
    final String in = tempDir.getRoot().toString() + "/testRoundTrip2/in";
    final String out = tempDir.getRoot().toString() + "/testRoundTrip2/out";
    final String verifyout = tempDir.getRoot().toString() + "/testRoundTrip2/verifyout";

    final int numRecords = 2;

    // Create a sequence file with the appropriate tuples
    Lfs lfsSource = new Lfs(new SequenceFile(testFields), in, SinkMode.REPLACE);
    TupleEntryCollector write = lfsSource.openForWrite(new HadoopFlowProcess());
    Tuple t = new Tuple();
    t.add(0);
    t.add(0L);
    t.add(false);
    t.add(0.0d);
    t.add(0.0f);
    t.add("0");
    CascadingToAvro.addToTuple(t, new byte[]{1});

    List<Long> arrayOfLongs = new ArrayList<Long>() {{
      add(0L);
    }};
    CascadingToAvro.addToTuple(t, arrayOfLongs);

    Map<String, String> mapOfStrings = new HashMap<String, String>() {{
      put("key-0", "value-0");
    }};
    CascadingToAvro.addToTuple(t, mapOfStrings);

    CascadingToAvro.addToTuple(t, TestEnum.ONE);
    write.add(t);

    t = new Tuple();
    t.add(1);
    t.add(1L);
    t.add(true);
    t.add(1.0d);
    t.add(1.0f);
    t.add("1");
    CascadingToAvro.addToTuple(t, new byte[]{1, 2});
    t.add(new Tuple(0L, 1L));
    t.add(new Tuple("key-0", "value-0", "key-1", "value-1"));
    CascadingToAvro.addToTuple(t, TestEnum.TWO);
    write.add(t);

    write.close();

    // Now read from the results, and write to an Avro file.
    Pipe writePipe = new Pipe("tuples to avro");

    Tap avroSink = new Lfs(new AvroScheme(testFields, schemeTypes), out);
    Flow flow = new HadoopFlowConnector().connect(lfsSource, avroSink, writePipe);
    flow.complete();

    // Now read it back in, and verify that the data/types match up.
    Tap avroSource = new Lfs(new AvroScheme(testFields, schemeTypes), out);


    TupleEntryIterator sinkTuples = avroSource.openForRead(new HadoopFlowProcess());
    assertTrue(sinkTuples.hasNext());

    int i = 0;
    while (sinkTuples.hasNext()) {
      TupleEntry te = sinkTuples.next();

      assertTrue(te.getObject("integerField") instanceof Integer);
      assertTrue(te.getObject("longField") instanceof Long);
      assertTrue(te.getObject("booleanField") instanceof Boolean);
      assertTrue(te.getObject("doubleField") instanceof Double);
      assertTrue(te.getObject("floatField") instanceof Float);
      assertTrue(te.getObject("stringField") instanceof String);
      assertTrue(te.getObject("bytesField") instanceof BytesWritable);
      assertTrue(te.getObject("arrayOfLongsField") instanceof List);
      assertTrue(te.getObject("mapOfStringsField") instanceof Map);
      assertTrue(te.getObject("enumField") instanceof String);

      assertEquals(i, te.getInteger("integerField"));
      assertEquals(i, te.getLong("longField"));
      assertEquals(i > 0, te.getBoolean("booleanField"));
      assertEquals((double) i, te.getDouble("doubleField"), 0.0001);
      assertEquals((float) i, te.getFloat("floatField"), 0.0001);
      assertEquals("" + i, te.getString("stringField"));
      assertEquals(i == 0 ? TestEnum.ONE : TestEnum.TWO, TestEnum.valueOf(te.getString("enumField")));

      BytesWritable bytesWritable = ((BytesWritable) te.getObject("bytesField"));
      byte[] bytes = ((BytesWritable) te.getObject("bytesField")).getBytes();
      for (int j = 0; j < i + 1; j++) {
        assertEquals(j + 1, bytes[j]);
      }

      List<Long> longArray = (List<Long>) te.getObject("arrayOfLongsField");
      assertEquals(i + 1, longArray.size());
      for (int j = 0; j < longArray.size(); j++) {
        assertTrue(longArray.get(j) instanceof Long);
        assertEquals(Long.valueOf(j), longArray.get(j));
      }

      Map<String, String> stringMap = (Map<String, String>) te.getObject("mapOfStringsField");
      int numMapEntries = i + 1;
      // Now make sure it has everything we're expecting.
      for (int j = 0; j < numMapEntries; j++) {
        assertEquals("value-" + j, stringMap.get("key-" + j));
      }

      i++;
    }

    assertEquals(numRecords, i);

    // Ensure that the Avro file we write out is readable via the standard Avro API
    File avroFile = new File(out + "/part-00000.avro");
    DataFileReader<Object> reader =
        new DataFileReader<Object>(avroFile, new GenericDatumReader<Object>());
    i = 0;
    while (reader.hasNext()) {
      reader.next();
      i++;
    }
    assertEquals(numRecords, i);

  }


  @Test
  public void testInvalidArrayData() {
    final Fields testFields = new Fields("arrayOfLongsField");
    final Class<?>[] schemeTypes = {List.class, Long.class};

    final String in = tempDir.getRoot().toString() + "/testInvalidArrayData/in";
    final String out = tempDir.getRoot().toString() + "/testInvalidArrayData/out";


    // Create a sequence file with the appropriate tuples
    Lfs lfsSource = new Lfs(new SequenceFile(testFields), in, SinkMode.REPLACE);
    TupleEntryCollector write;
    try {
      write = lfsSource.openForWrite(new HadoopFlowProcess());
      Tuple t = new Tuple();
      t.add(new Tuple(0L, "invalid data type"));
      write.add(t);
      write.close();
      // Now read from the results, and write to an Avro file.
      Pipe writePipe = new Pipe("tuples to avro");

      Tap avroSink = new Lfs(new AvroScheme(testFields, schemeTypes), out);
      Flow flow = new HadoopFlowConnector().connect(lfsSource, avroSink, writePipe);
      flow.complete();
      fail("Exception should be thrown as there is an invalid array element");

    } catch (Exception e) {
      // Ignore.
    }
  }

  @Test
  public void testInvalidMap() {
    final Fields testFields = new Fields("mapOfStringsField");
    final Class<?>[] schemeTypes = {Map.class, String.class};

    final String in = tempDir.getRoot().toString() + "/testInvalidMap/in";
    final String out = tempDir.getRoot().toString() + "/testInvalidMap/out";

    // Create a sequence file with the appropriate tuples
    Lfs lfsSource = new Lfs(new SequenceFile(testFields), in, SinkMode.REPLACE);
    TupleEntryCollector write;
    try {
      write = lfsSource.openForWrite(new HadoopFlowProcess());
      Tuple t = new Tuple();
      // add invalid map data - where only key is present and no value
      t.add(new Tuple("key-0", "value-0", "key-1"));
      write.add(t);
      write.close();
      // Now read from the results, and write to an Avro file.
      Pipe writePipe = new Pipe("tuples to avro");

      Tap avroSink = new Lfs(new AvroScheme(testFields, schemeTypes), out);
      Flow flow = new HadoopFlowConnector().connect(lfsSource, avroSink, writePipe);
      flow.complete();
      fail("Exception should be thrown as there is an invalid map");

    } catch (Exception e) {
      // Ignore.
    }
  }

  @Test
  public void testInvalidMapData() {
    final Fields testFields = new Fields("mapOfStringsField");
    final Class<?>[] schemeTypes = {Map.class, String.class};

    final String in = tempDir.getRoot().toString() + "/testInvalidMapData/in";
    final String out = tempDir.getRoot().toString() + "/testInvalidMapData/out";

    // Create a sequence file with the appropriate tuples
    Lfs lfsSource = new Lfs(new SequenceFile(testFields), in, SinkMode.REPLACE);
    TupleEntryCollector write;
    try {
      write = lfsSource.openForWrite(new HadoopFlowProcess());
      Tuple t = new Tuple();
      // add invalid map data - key isn't a String
      t.add(new Tuple("key-0", "value-0", 1L, "value-2"));
      write.add(t);
      write.close();
      // Now read from the results, and write to an Avro file.
      Pipe writePipe = new Pipe("tuples to avro");

      Tap avroSink = new Lfs(new AvroScheme(testFields, schemeTypes), out);
      Flow flow = new HadoopFlowConnector().connect(lfsSource, avroSink, writePipe);
      flow.complete();
      fail("Exception should be thrown as the key isn't a String");

    } catch (Exception e) {
      // Ignore.
    }
  }

  @Test
  public void testNullable() throws Exception {
    final Fields testFields = new Fields("nullString");
    final Class<?>[] schemeTypes = {String.class};

    final String in = tempDir.getRoot().toString() + "/testNullable/in";
    final String out = tempDir.getRoot().toString() + "/testNullable/out";

    // Create a sequence file with the appropriate tuples
    Lfs lfsSource = new Lfs(new SequenceFile(testFields), in, SinkMode.REPLACE);
    TupleEntryCollector write;

    write = lfsSource.openForWrite(new HadoopFlowProcess());
    Tuple t = new Tuple();
    String nullString = null;
    t.add(nullString);
    write.add(t);
    write.close();
    // Now read from the results, and write to an Avro file.
    Pipe writePipe = new Pipe("tuples to avro");

    Tap avroSink = new Lfs(new AvroScheme(testFields, schemeTypes), out);
    Flow flow = new HadoopFlowConnector().connect(lfsSource, avroSink, writePipe);
    flow.complete();
  }

  // @Test
  // public void testSetRecordName() {
  //     AvroScheme avroScheme = new AvroScheme(new Fields("a"), new Class[] { Long.class });
  //     String expected = "{\"type\":\"record\",\"name\":\"CascadingAvroRecord\",\"namespace\":\"\",\"fields\":[{\"name\":\"a\",\"type\":[\"null\",\"long\"],\"doc\":\"\"}]}";
  //     String jsonSchema = avroScheme.getJsonSchema();
  //     assertEquals(expected, jsonSchema);
  //     avroScheme.setRecordName("FooBar");
  //     String jsonSchemaWithRecordName = avroScheme.getJsonSchema();
  //     String expectedWithName = "{\"type\":\"record\",\"name\":\"FooBar\",\"namespace\":\"\",\"fields\":[{\"name\":\"a\",\"type\":[\"null\",\"long\"],\"doc\":\"\"}]}";
  //     assertEquals(expectedWithName, jsonSchemaWithRecordName);
  // }

  @Test
  public void testEnumInSchema() throws Exception {
    AvroScheme avroScheme = new AvroScheme(new Fields("a"), new Class[]{TestEnum.class});
    String jsonSchema = avroScheme.getJsonSchema();
    String enumField = String.format("{\"type\":\"enum\",\"name\":\"%s\",\"namespace\":\"%s\",\"symbols\":[\"ONE\",\"TWO\"]}",
                                     "TestEnum", TestEnum.class.getPackage().getName());

    String expected = String.format("{\"type\":\"record\",\"name\":\"CascadingAvroRecord\",\"doc\":\"auto generated\",\"fields\":[{\"name\":\"a\",\"type\":[\"null\",%s],\"doc\":\"\"}]}",
                                    enumField);
    assertEquals(expected, jsonSchema);
  }

  @Test
  public void groupByAvroReadWriteTest() throws Exception {
    String docPath = getClass().getResource("words.txt").getPath();
    String wcPath = tempDir.getRoot().toString() + "/testGrouped/out/";
    String finalPath = tempDir.getRoot().toString() + "/testGrouped/final/";

    // Get the schema from a file
    Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("test6.avsc"));

    HadoopFlowConnector flowConnector = new HadoopFlowConnector();

    // create source and sink taps
    Tap docTap = new Lfs(new TextLine(new Fields("text")), docPath);
    Tap wcTap = new Lfs(new AvroScheme(schema), wcPath, SinkMode.REPLACE);

    // specify a regex operation to split the "document" text lines into a token stream
    Fields token = new Fields("token");
    Fields text = new Fields("text");
    RegexSplitGenerator splitter = new RegexSplitGenerator(token, "[ \\[\\]\\(\\),.]");
    // only returns "token"
    Pipe docPipe = new Each("token", text, splitter, Fields.RESULTS);

    // determine the word counts
    Pipe wcPipe = new Pipe("wc", docPipe);
    wcPipe = new GroupBy(wcPipe, token);
    wcPipe = new Every(wcPipe, Fields.ALL, new Count(), Fields.ALL);

    // connect the taps, pipes, etc., into a flow
    FlowDef flowDef = FlowDef.flowDef()
        .setName("wc")
        .addSource(docPipe, docTap)
        .addTailSink(wcPipe, wcTap);

    Flow wcFlow = flowConnector.connect(flowDef);
    wcFlow.complete();

    HadoopFlowConnector flowConnector2 = new HadoopFlowConnector();
    // create source and sink taps
    // Source is Avro, note there is no schema needed. 
    Tap avroTap = new Lfs(new AvroScheme(), wcPath);
    Tap finalTap = new Lfs(new TextDelimited(), finalPath, SinkMode.REPLACE);

    Pipe avroPipe = new Pipe("wordcount");
    avroPipe = new GroupBy(avroPipe, new Fields("count"));
    avroPipe = new Every(avroPipe, Fields.ALL, new Count(new Fields("countcount")), Fields.ALL);


    // connect the taps, pipes, etc., into a flow
    FlowDef flowDef2 = FlowDef.flowDef()
        .setName("wc-read")
        .addSource(avroPipe, avroTap)
        .addTailSink(avroPipe, finalTap);

    // write a DOT file and run the flow
    Flow wcFlow2 = flowConnector2.connect(flowDef2);
    wcFlow2.complete();

  }

  @Test
  public void testSerializeExtraLargeSchema() throws Exception {
    final Schema extraLarge = new Schema.Parser().parse(getClass().getResourceAsStream(
            "test-extra-large.avsc"));
    final AvroScheme expected = new AvroScheme(extraLarge);

    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(bytes);
    oos.writeObject(expected);
    oos.close();

    final ObjectInputStream iis = new ObjectInputStream(
            new ByteArrayInputStream(bytes.toByteArray()));
    final AvroScheme actual = (AvroScheme) iis.readObject();

    assertEquals(expected, actual);
  }
}
