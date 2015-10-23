package cascading.avro.local;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextDelimited;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.*;
import junit.framework.Assert;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class LocalSchemeTest extends Assert {

    @Rule
    public final TemporaryFolder tempDir = new TemporaryFolder();

    @Test
    public void testRoundTrip() throws Exception {

        final Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream(
                "../test1.avsc"));

        final Fields fields = new Fields("aBoolean", "anInt", "aLong",
                "aDouble", "aFloat", "aBytes", "aFixed", "aNull", "aString",
                "aList", "aMap", "aUnion");

        String output = tempDir.getRoot().toString() + "/testRoundTrip/output";
        String input = tempDir.getRoot().toString() + "/testRoundTrip/input";
        Tap lfsSource = new FileTap(new cascading.avro.local.AvroScheme(schema), input, SinkMode.REPLACE);
        TupleEntryCollector write = lfsSource.openForWrite(new LocalFlowProcess());

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

        Tap avroSink = new FileTap(new cascading.avro.local.AvroScheme(schema), output);
        Flow flow = new LocalFlowConnector().connect(lfsSource, avroSink, writePipe);
        flow.complete();

        // Now read it back in, and verify that the data/types match up.
        Tap avroSource = new FileTap(new cascading.avro.local.AvroScheme(schema), output);


        TupleEntryIterator iterator = avroSource.openForRead(new LocalFlowProcess());

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
    public void groupByAvroReadWriteTest() throws Exception {

        String docPath = getClass().getResource("../words.txt").getPath();
        String wcPath = tempDir.getRoot().toString() + "/testGrouped/out";
        String finalPath = tempDir.getRoot().toString() + "/testGrouped/final";

        // Get the schema from a file
        Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("../test6.avsc"));

        LocalFlowConnector flowConnector = new LocalFlowConnector();

        // create source and sink taps
        Tap docTap = new FileTap(new TextLine(new Fields("text")), docPath);
        Tap wcTap = new FileTap(new cascading.avro.local.AvroScheme(schema), wcPath);

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

        LocalFlowConnector flowConnector2 = new LocalFlowConnector();
        // create source and sink taps
        // Source is Avro, note there is no schema needed.
        Tap avroTap = new FileTap(new cascading.avro.local.AvroScheme(), wcPath);
        Tap finalTap = new FileTap(new TextDelimited(), finalPath);

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

        BufferedReader br = new BufferedReader(new FileReader(finalPath));
        List<String> lines = new ArrayList<String>();
        String line = br.readLine();
        while (line != null) {
            lines.add(line);
            line = br.readLine();
        }
        assertEquals(2, lines.size());
        assertEquals(lines.get(0), "1\t4");
        assertEquals(lines.get(1), "4\t1");
    }

    @Test
    public void notUnpackedTest() throws Exception {
        final Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream(
                "../test2.avsc"));
        String in = tempDir.getRoot().toString() + "/recordtest/in";
        String out = tempDir.getRoot().toString() + "/recordtest/out";
        Tap lfsSource = new FileTap(new cascading.avro.local.PackedAvroScheme<GenericData.Record>(schema), in, SinkMode.REPLACE);
        TupleEntryCollector write = lfsSource.openForWrite(new LocalFlowProcess());
        Tuple tuple = Tuple.size(1);
        GenericData.Record record = new GenericData.Record(schema);
        String innerSchema = "{\"type\":\"record\", \"name\":\"nested\", \"namespace\": \"cascading.avro\", \"fields\": [{\"name\":\"anInnerField1\", \"type\":\"int\"}, {\"name\":\"anInnerField2\", \"type\":\"string\"} ] }";
        GenericData.Record inner = new GenericData.Record(new Schema.Parser().parse(innerSchema));
        inner.put(0, 0);
        inner.put(1, "the string");
        record.put(0, inner);
        record.put(1, "outer string");
        tuple.set(0, record);
        TupleEntry te = new TupleEntry(new Fields("test2"), tuple);
        write.add(te);
        write.close();


        Pipe writePipe = new Pipe("tuples to avro");

        Tap avroSink = new FileTap(new cascading.avro.local.PackedAvroScheme<GenericData.Record>(schema), out);
        Flow flow = new LocalFlowConnector().connect(lfsSource, avroSink, writePipe);
        flow.complete();

        // Now read it back in, and verify that the data/types match up.
        Tap avroSource = new FileTap(new cascading.avro.local.PackedAvroScheme<GenericData.Record>(schema), out);


        TupleEntryIterator iterator = avroSource.openForRead(new LocalFlowProcess());

        assertTrue(iterator.hasNext());
        final TupleEntry readEntry1 = iterator.next();

        assertTrue(readEntry1.getObject(0) instanceof IndexedRecord);
        assertEquals(((IndexedRecord) record.get(0)).get(0), ((IndexedRecord) ((IndexedRecord) readEntry1.getObject(0)).get(0)).get(0));
        assertEquals(new Utf8((String) ((IndexedRecord) record.get(0)).get(1)), ((IndexedRecord) ((IndexedRecord) readEntry1.getObject(0)).get(0)).get(1));
        assertEquals(new Utf8((String) record.get(1)), ((IndexedRecord) readEntry1.getObject(0)).get(1));

    }


}
