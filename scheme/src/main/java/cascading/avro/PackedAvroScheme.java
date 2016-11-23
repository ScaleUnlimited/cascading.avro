package cascading.avro;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import abracad.avro.ClojureData;
import org.apache.avro.hadoop.io.AvroSerialization;

import java.io.IOException;

public class PackedAvroScheme<T> extends AvroScheme {
  /**
   * This scheme should be used when you don't want cascading.avro to automatically unpack or pack your Avro objects.
   * The constructors are similar to the super class but there is only ever one field incoming or outgoing. The parameter
   * is the type of Avro record to read.
   */

  protected boolean clojure = false;

  public PackedAvroScheme() {
    this(null);
  }

  /**
   * Constructs a scheme from an Avro schema and names the single field using the Schema name.
   *
   * @param schema The avro schema to use
   */
  public PackedAvroScheme(Schema schema) {
    this.schema = schema;
//    if (schema == null) {
    setSinkFields(Fields.FIRST);
    setSourceFields(Fields.FIRST);
  }
  
  public PackedAvroScheme(Schema schema, boolean clojure) {
    this(schema);
    this.clojure = clojure;
  }
  
  /**
     * sourceConfInit is called by cascading to set up the sources. This happens on the client side before the
     * job is distributed.
     * There is a check for the presence of a schema and if none has been provided the data is peeked at to get a schema.
     * After the schema check the conf object is given the options that Avro needs.
     *
     * @param flowProcess The cascading FlowProcess object. Should be passed in by cascading automatically.
     * @param tap         The cascading Tap object. Should be passed in by cascading automatically.
     * @param conf        The Hadoop JobConf object. This is passed in by cascading automatically.
     * @throws RuntimeException If no schema is present this halts the entire process.
     */
  @Override
  public void sourceConfInit(
    FlowProcess<JobConf> flowProcess,
    Tap<JobConf, RecordReader, OutputCollector> tap,
    JobConf conf) {
    super.sourceConfInit(flowProcess, tap, conf);
    if (clojure) {
      AvroSerialization.setDataModelClass(conf, ClojureData.class);
    }
  }
  
  /**
     * sinkConfInit is called by cascading to set up the sinks. This happens on the client side before the
     * job is distributed.
     * There is a check for the presence of a schema and an exception is thrown if none has been provided.
     * After the schema check the conf object is given the options that Avro needs.
     *
     * @param flowProcess The cascading FlowProcess object. Should be passed in by cascading automatically.
     * @param tap         The cascading Tap object. Should be passed in by cascading automatically.
     * @param conf        The Hadoop JobConf object. This is passed in by cascading automatically.
     * @throws RuntimeException If no schema is present this halts the entire process.
     */
  @Override
  public void sinkConfInit(
    FlowProcess<JobConf> flowProcess,
    Tap<JobConf, RecordReader, OutputCollector> tap,
    JobConf conf) {
    super.sinkConfInit(flowProcess, tap, conf);
    if (clojure) {
      AvroSerialization.setDataModelClass(conf, ClojureData.class);
    }
  }
  

  /**
   * Sink method to take an outgoing tuple and write it to Avro. In this scheme the incoming avro is passed through.
   *
   * @param flowProcess The cascading FlowProcess object. Should be passed in by cascading automatically.
   * @param sinkCall    The cascading SinkCall object. Should be passed in by cascading automatically.
   * @throws java.io.IOException
   */
  @Override
  public void sink(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
    TupleEntry tupleEntry = sinkCall.getOutgoingEntry();
    //noinspection unchecked
    sinkCall.getOutput().collect(new AvroWrapper<T>((T) tupleEntry.getObject(Fields.FIRST)), NullWritable.get());
  }

  /**
   * In this schema nothing needs to be done for the sinkPrepare.
   *
   * @param flowProcess The cascading FlowProcess object. Should be passed in by cascading automatically.
   * @param sinkCall    The cascading SinkCall object. Should be passed in by cascading automatically.
   * @throws java.io.IOException
   */
  @Override
  public void sinkPrepare(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall)
      throws IOException {
  }

  /**
   * Sets to Fields.UNKNOWN if no schema is present, otherwise uses the name of the Schema.
   *
   * @param flowProcess The cascading FlowProcess object. Should be passed in by cascading automatically.
   * @param tap         The cascading Tap object. Should be passed in by cascading automatically.
   * @return Fields The source cascading fields.
   */
  @Override
  public Fields retrieveSourceFields(FlowProcess<JobConf> flowProcess, Tap tap) {
    if (schema == null) {
      setSourceFields(Fields.UNKNOWN);
    } else {
      setSourceFields(new Fields(schema.getName()));
    }
    return getSourceFields();
  }

  /**
   * Reads in Avro records of type T and adds them as the first field in a tuple.
   *
   * @param flowProcess The cascading FlowProcess object. Should be passed in by cascading automatically.
   * @param sourceCall  The cascading SourceCall object. Should be passed in by cascading automatically.
   * @return boolean true on successful parsing and collection, false on failure.
   * @throws java.io.IOException
   */
  @Override
  public boolean source(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall) throws IOException {
    @SuppressWarnings("unchecked") RecordReader<AvroWrapper<T>, Writable> input = sourceCall.getInput();
    AvroWrapper<T> wrapper = input.createKey();
    if (!input.next(wrapper, input.createValue())) {
      return false;
    }
    T record = wrapper.datum();
    Tuple tuple = sourceCall.getIncomingEntry().getTuple();
    tuple.clear();
    tuple.add(record);
    return true;
  }
}
