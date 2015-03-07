package cascading.avro.serialization;

import cascading.avro.AvroScheme;
import cascading.avro.generated.Test1;
import cascading.avro.generated.TestEnum;
import cascading.avro.generated.md51;
import cascading.tuple.Tuple;
import cascading.tuple.hadoop.TupleSerialization;
import cascading.tuple.hadoop.io.TupleDeserializer;
import cascading.tuple.hadoop.io.TupleSerializer;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.avro.generic.GenericData;
import org.apache.commons.compress.utils.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;

public class AvroSpecificDataSerializationTest {
  private Test1 test;
  private md51 md5;
  private TestEnum testEnum;

  @Before
  public void setupAvroObjects() {
    HashFunction hf = Hashing.md5();
    HashCode hc = hf.newHasher()
        .putLong(23423434234l)
        .putString("Fooness", Charsets.UTF_8)
        .hash();
    md5 = new md51(hc.asBytes());
    test = Test1.newBuilder()
        .setAFixed(md5)
        .setABoolean(true)
        .setANull(null)
        .setAList(new ArrayList<Integer>())
        .setAMap(new HashMap<CharSequence, Integer>())
        .setAUnion(2)
        .setABytes(ByteBuffer.wrap(new byte[]{3, 4}))
        .build();
    testEnum = TestEnum.valueOf("ONE");
  }

  @Test
  public void testRoundTrip() throws IOException {
    roundTripEquals(test, new Test1());
    roundTripEquals(md5, new md51());
    roundTripEquals(testEnum, null);
    roundTripEquals(test, null);
    roundTripEquals(md5, null);
  }

  @Test
  public void testOnlyAcceptsSpecificData() {
    Serialization serialization = new AvroSpecificDataSerialization();
    assertFalse(serialization.accept(int[].class));
    assertFalse(serialization.accept(List.class));
    assertFalse(serialization.accept(GenericData.Record.class));
    assertFalse(serialization.accept(Long.class));
  }

  @Test
  public void testDirectUnbufferedEncodingAndDecoding() throws IOException {
    JobConf conf = new JobConf();
    AvroScheme.addAvroSerializations(conf);

    TupleSerialization tupleSerialization = new TupleSerialization(conf);
    TupleSerializer tupleSerializer = new TupleSerializer(tupleSerialization.getElementWriter());
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Tuple tuple = new Tuple("foo", test, 2, testEnum, 34l);
    tupleSerializer.open(out);
    tupleSerializer.serialize(tuple);
    out.close();

    TupleDeserializer tupleDeserializer = new TupleDeserializer(tupleSerialization.getElementReader());
    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    tupleDeserializer.open(in);
    Tuple tupleOut = new Tuple();
    tupleDeserializer.deserialize(tupleOut);
    tupleDeserializer.close();

    assertEquals(tuple, tupleOut);
  }

  private void roundTripEquals(Object test, Object testOut) throws IOException {
    Serialization serialization = new AvroSpecificDataSerialization();

    if (!serialization.accept(test.getClass())) fail();
    Serializer serializer = serialization.getSerializer(test.getClass());
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    serializer.open(out);
    serializer.serialize(test);
    serializer.close();

    Deserializer deserializer = serialization.getDeserializer(test.getClass());
    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    deserializer.open(in);
    testOut = deserializer.deserialize(testOut);
    deserializer.close();

    assertEquals(test, testOut);
  }
}
