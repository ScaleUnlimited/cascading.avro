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

import cascading.avro.conversion.AvroConverter;
import cascading.avro.conversion.AvroToCascading;
import cascading.avro.conversion.CascadingToAvro;
import cascading.avro.serialization.AvroSpecificDataSerialization;
import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.CompositeTap;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;


public class AvroScheme extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {
    protected Schema schema;
    protected int recordUnpackDepth;
    protected AvroConverter<IndexedRecord, TupleEntry> avroToCascading;
    protected AvroConverter<TupleEntry, IndexedRecord> cascadingToAvro;

    private static final String HADOOP_SERIALIZATIONS_KEY = "io.serializations";
    private static final String DEFAULT_RECORD_NAME = "CascadingAvroRecord";
    private static final PathFilter filter = new PathFilter() {
        @Override
        public boolean accept(Path path) {
            return !path.getName().startsWith("_");
        }
    };

    /**
     * Constructor to read from an Avro source or write to an Avro sink without specifying the schema. If using as a sink,
     * the sink Fields must have type information and currently Map and List are not supported.
     */
    public AvroScheme() {
        this(null, -1);
    }

    public AvroScheme(int recordUnpackDepth) {
        this(null, recordUnpackDepth);
    }

    public AvroScheme(Schema schema) {
        this(schema, -1);
    }

    /**
     * Create a new Cascading 2.0 scheme suitable for reading and writing data using the Avro serialization format.
     * This is the legacy constructor format. A Fields object and the corresponding types must be provided.
     *
     * @param fields Fields object from cascading
     * @param types  array of Class types
     */
    public AvroScheme(Fields fields, Class<?>[] types) {
        this(AvroSchemata.generateAvroSchemaFromFieldsAndTypes(DEFAULT_RECORD_NAME, fields, types), -1);
    }


    public AvroScheme(Schema schema, int recordUnpackDepth) {
        this(schema, recordUnpackDepth, null, null);
    }

    /**
     * Create a new Cascading 2.0 scheme suitable for reading and writing data using the Avro serialization format.
     * Note that if schema is null, the Avro schema will be inferred from one of the source files (if this scheme
     * is being used as a source). At the moment, we are unable to infer a schema for a sink (this will change soon with
     * a new version of cascading though).
     *
     * @param schema            Avro schema, or null if this is to be inferred from source file. Note that a runtime exception will happen
     *                          if the AvroScheme is used as a sink and no schema is supplied.
     * @param recordUnpackDepth
     * @param avroToCascading
     */
    public AvroScheme(Schema schema,
                      int recordUnpackDepth,
                      AvroConverter<IndexedRecord, TupleEntry> avroToCascading,
                      AvroConverter<TupleEntry, IndexedRecord> cascadingToAvro) {
        this.recordUnpackDepth = recordUnpackDepth;
        this.schema = schema;
        this.avroToCascading = avroToCascading != null ? avroToCascading : new AvroToCascading();
        this.cascadingToAvro = cascadingToAvro != null ? cascadingToAvro : new CascadingToAvro();
        if (recordUnpackDepth == 0) {
            // Singleton tuple
            setSinkFields(Fields.FIRST);
            setSourceFields(Fields.FIRST);
        }
        else if (schema == null) {
            this.setSinkFields(Fields.ALL);
            this.setSourceFields(Fields.UNKNOWN);
        }
        else {
            Fields cascadingFields = this.getCascadingFields();
            this.setSinkFields(cascadingFields);
            this.setSourceFields(cascadingFields);
        }
    }

    protected Fields getCascadingFields() {
        List<Schema.Field> avroFields = this.schema.getFields();
        Comparable[] fieldNames = new String[avroFields.size()];
        for (int i = 0; i < avroFields.size(); i++) {
            Schema.Field avroField = avroFields.get(i);
            fieldNames[i] = avroField.name();
        }
        return new Fields(fieldNames);
    }

    /**
     * Return the schema which has been set as a string
     *
     * @return String representing the schema
     */
    String getJsonSchema() {
        if (schema == null) {
            return "";
        }
        else {
            return schema.toString();
        }
    }

    /**
     * Sink method to take an outgoing tuple and write it to Avro.
     *
     * @param flowProcess The cascading FlowProcess object. Should be passed in by cascading automatically.
     * @param sinkCall    The cascading SinkCall object. Should be passed in by cascading automatically.
     * @throws IOException
     */
    @Override
    public void sink(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall)
        throws IOException {
        TupleEntry tupleEntry = sinkCall.getOutgoingEntry();
        Schema schema = (Schema) sinkCall.getContext()[0];
        IndexedRecord record = this.recordUnpackDepth == 0 ?
            (IndexedRecord) tupleEntry.getObject(Fields.FIRST) :
            this.cascadingToAvro.convertRecord(tupleEntry, schema, this.recordUnpackDepth);
        //noinspection unchecked
        sinkCall.getOutput().collect(new AvroWrapper<IndexedRecord>(record), NullWritable.get());
    }

    protected Function<Schema, IndexedRecord> avroRecordBuilder() {
        return new Function<Schema, IndexedRecord>() {
            @Override
            public IndexedRecord apply(Schema schema) {
                return new Record(schema);
            }
        };
    }

    /**
     * Sink prepare method called by cascading once on each reducer. This method stuffs the schema into a context
     * for easy access by the sink method.
     *
     * @param flowProcess The cascading FlowProcess object. Should be passed in by cascading automatically.
     * @param sinkCall    The cascading SinkCall object. Should be passed in by cascading automatically.
     * @throws IOException
     */
    @Override
    public void sinkPrepare(
        FlowProcess<JobConf> flowProcess,
        SinkCall<Object[], OutputCollector> sinkCall)
        throws IOException {
        sinkCall.setContext(new Object[]{schema});
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

        if (schema == null) {
            throw new RuntimeException("Must provide sink schema");
        }
        // Set the output schema and output format class
        conf.set(AvroJob.OUTPUT_SCHEMA, schema.toString());
        conf.setOutputFormat(AvroOutputFormat.class);

        // add AvroSerialization to io.serializations
        addAvroSerializations(conf);
    }

    /**
     * This method is called by cascading to set up the incoming fields. If a schema isn't present then it will
     * go and peek at the input data to retrieve one. The field names from the schema are used to name the cascading fields.
     *
     * @param flowProcess The cascading FlowProcess object. Should be passed in by cascading automatically.
     * @param tap         The cascading Tap object. Should be passed in by cascading automatically.
     * @return Fields The source cascading fields.
     */
    @Override
    public Fields retrieveSourceFields(FlowProcess<JobConf> flowProcess, Tap tap) {
        if (schema == null) {
            try {
                schema = getSourceSchema(flowProcess, tap);
            }
            catch (IOException e) {
                throw new RuntimeException("Can't get schema from data source");
            }
        }
        Fields cascadingFields = new Fields();
        if (schema.getType().equals(Schema.Type.NULL)) {
            cascadingFields = Fields.NONE;
        }
        else {
            for (Field avroField : schema.getFields())
                cascadingFields = cascadingFields.append(new Fields(avroField.name()));
        }
        setSourceFields(cascadingFields);
        return getSourceFields();
    }

    /**
     * Source method to take an incoming Avro record and make it a Tuple.
     *
     * @param flowProcess The cascading FlowProcess object. Should be passed in by cascading automatically.
     * @param sourceCall  The cascading SourceCall object. Should be passed in by cascading automatically.
     * @return boolean true on successful parsing and collection, false on failure.
     * @throws IOException
     */
    @Override
    public boolean source(
        FlowProcess<JobConf> flowProcess,
        SourceCall<Object[], RecordReader> sourceCall)
        throws IOException {

        @SuppressWarnings("unchecked") RecordReader<AvroWrapper<IndexedRecord>, Writable> input = sourceCall.getInput();
        AvroWrapper<IndexedRecord> wrapper = input.createKey();
        if (!input.next(wrapper, input.createValue())) {
            return false;
        }
        IndexedRecord record = wrapper.datum();

        if (this.recordUnpackDepth == 0) {
            Tuple tuple = sourceCall.getIncomingEntry().getTuple();
            tuple.clear();
            tuple.add(record);
        }
        else {
            this.avroToCascading.convertRecord(sourceCall.getIncomingEntry(), record, schema, this.recordUnpackDepth);
        }

        return true;
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

        retrieveSourceFields(flowProcess, tap);
        // Set the input schema and input class
        conf.set(AvroJob.INPUT_SCHEMA, schema.toString());
        conf.setInputFormat(AvroInputFormat.class);

        // add AvroSerialization to io.serializations
        addAvroSerializations(conf);
    }

    /* TODO: this is a bit ugly does it really belong here? what if we have files with different schemas? */

    /**
     * This method peeks at the source data to get a schema when none has been provided.
     *
     * @param flowProcess The cascading FlowProcess object for this flow.
     * @param tap         The cascading Tap object.
     * @return Schema The schema of the peeked at data, or Schema.NULL if none exists.
     */
    private Schema getSourceSchema(FlowProcess<JobConf> flowProcess, Tap tap) throws IOException {

        if (tap instanceof CompositeTap) {
            tap = (Tap) ((CompositeTap) tap).getChildTaps().next();
        }
        final String path = tap.getIdentifier();
        Path p = new Path(path);
        final FileSystem fs = p.getFileSystem(flowProcess.getConfigCopy());
        // Get all the input dirs
        List<FileStatus> statuses = new LinkedList<FileStatus>(Arrays.asList(fs.globStatus(p, filter)));
        // Now get all the things that are one level down
        for (FileStatus status : new LinkedList<FileStatus>(statuses)) {
            if (status.isDir())
                for (FileStatus child : Arrays.asList(fs.listStatus(status.getPath(), filter))) {
                    if (child.isDir()) {
                        statuses.addAll(Arrays.asList(fs.listStatus(child.getPath(), filter)));
                    }
                    else if (fs.isFile(child.getPath())) {
                        statuses.add(child);
                    }
                }
        }
        for (FileStatus status : statuses) {
            Path statusPath = status.getPath();
            if (fs.isFile(statusPath)) {
                // no need to open them all
                InputStream stream = null;
                DataFileStream reader = null;
                try {
                    stream = new BufferedInputStream(fs.open(statusPath));
                    reader = new DataFileStream(stream, new GenericDatumReader());
                    return reader.getSchema();
                }
                finally {
                    if (reader == null) {
                        if (stream != null) {
                            stream.close();
                        }
                    }
                    else {
                        reader.close();
                    }
                }

            }
        }
        // couldn't find any Avro files, return null schema
        return Schema.create(Schema.Type.NULL);
    }

    public static ImmutableList<String> getSerializationClassNames() {
        return ImmutableList.<String>builder()
            .add(AvroSerialization.class.getName())
            .add(AvroSpecificDataSerialization.class.getName())
            .build();
    }

    /**
     * Adds Avro serializations for AvroWrapper and Avro SpecificData-served types in front of other serializations
     * set in io.serializations.
     *
     *
     * @param conf The Configuration to add serializations to.
     */
    public static void addAvroSerializations(Configuration conf) {
        conf.set(HADOOP_SERIALIZATIONS_KEY, addAvroSerializations(conf.get(HADOOP_SERIALIZATIONS_KEY)));
    }

    /**
     * Adds Avro serializations for AvroWrapper and Avro SpecificData-served types in front of other
     * serializations set in io.serializations, assuming io.serializations is a comma-separated string.
     *
     * @param props The Properties to add serializations to.
     */
    public static void addAvroSerializations(Properties props) {
        props.put(HADOOP_SERIALIZATIONS_KEY,
            addAvroSerializations((String) props.get(HADOOP_SERIALIZATIONS_KEY)));
    }

    public static String serializationClassNamesString() {
        return addAvroSerializations("");
    }

    public static String addAvroSerializations(String serializationsString) {
        Joiner joiner = Joiner.on(',').skipNulls();
        Splitter splitter = Splitter.on(',').omitEmptyStrings();
        if (serializationsString == null) serializationsString = "";
        Iterable<String> serializations = ImmutableSet.copyOf(Iterables.concat(getSerializationClassNames(),
            splitter.split(serializationsString)));
        return joiner.join(serializations);
    }

    // Serializability of this Scheme
    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeObject(this.schema.toString());
        out.writeInt(this.recordUnpackDepth);
        out.writeObject(this.avroToCascading);
        out.writeObject(this.cascadingToAvro);
    }

    @SuppressWarnings({"unchecked"})
    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        this.schema = new Schema.Parser().parse((String) in.readObject());
        this.recordUnpackDepth = in.readInt();
        this.avroToCascading = (AvroConverter) in.readObject();
        this.cascadingToAvro = (AvroConverter) in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        AvroScheme that = (AvroScheme) o;

        if (schema != null ? !schema.equals(that.schema) : that.schema != null) return false;

        return true;
    }

    @Override
    public String toString() {
        return "AvroScheme{" +
            "schema=" + schema +
            '}';
    }

    @Override
    public int hashCode() {
        return 31 * getSinkFields().hashCode() +
            (schema == null ? 0 : schema.hashCode());
    }
}


