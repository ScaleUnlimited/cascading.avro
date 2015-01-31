package cascading.avro.local;

import cascading.avro.conversion.AvroConverter;
import cascading.avro.conversion.AvroToCascading;
import cascading.avro.conversion.CascadingToAvro;
import cascading.avro.conversion.TypeMappings;
import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.CompositeTap;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import com.google.common.base.Function;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * A local version of the Cascading Scheme for Apache Avro.
 */
public class AvroScheme extends Scheme<Properties, InputStream, OutputStream, DataFileStream, DataFileWriter> {
    private static final Logger LOG = LoggerFactory.getLogger(AvroScheme.class);
    protected final int recordUnpackDepth;
    protected AvroConverter<IndexedRecord, TupleEntry> avroToCascading;
    protected AvroConverter<TupleEntry, IndexedRecord> cascadingToAvro;
    protected Encoder encoder;
    protected Schema schema;
    protected GenericDatumReader<IndexedRecord> datumReader;
    protected GenericDatumWriter<IndexedRecord> datumWriter;

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
     * Note that if schema is null, the Avro schema will be inferred from one of the source files (if this scheme
     * is being used as a source). At the moment, we are unable to infer a schema for a sink (this will change soon with
     * a new version of cascading though).
     *
     * @param schema            Avro schema, or null if this is to be inferred from source file. Note that a runtime exception will happen
     *                          if the AvroScheme is used as a sink and no schema is supplied.
     * @param recordUnpackDepth
     */
    public AvroScheme(Schema schema, int recordUnpackDepth) {
        this.avroToCascading = new AvroToCascading();
        this.cascadingToAvro = new CascadingToAvro();
        this.recordUnpackDepth = recordUnpackDepth;
        if (schema == null) {
            this.setSinkFields(Fields.ALL);
            this.setSourceFields(Fields.UNKNOWN);
        }
        else {
            this.schema = schema;
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

    DataFileStream<IndexedRecord> createInput(InputStream inputStream) {
        if (datumReader == null)
            datumReader = new GenericDatumReader<IndexedRecord>(schema);
        try {
            return new DataFileStream<IndexedRecord>(inputStream, datumReader);
        }
        catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        // should never reach here
        return null;
    }

    DataFileWriter<IndexedRecord> createOutput(OutputStream outputStream) {
        if (encoder == null)
            encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

        if (datumWriter == null)
            datumWriter = new GenericDatumWriter<IndexedRecord>(schema);

        DataFileWriter<IndexedRecord> dataWriter = new DataFileWriter<IndexedRecord>(datumWriter);

        try {
            dataWriter.create(schema, outputStream);
            return dataWriter;
        }
        catch (IOException e) {
            LOG.error("Unable to create the DataFileWriter output.");
            e.printStackTrace();
            System.exit(1);
        }
        //should never get here
        return null;
    }

    /**
     * Method retrieveSourceFields will build a new Cascading Fields object out of the Avro Schema if it exists. If
     * the Avro Scheme does not exist it will go and peek at the file to be read and use that Schema.
     *
     * @param process
     * @param tap
     * @return Fields a cascading fields object with one field per Avro record field
     */
    @Override
    public Fields retrieveSourceFields(FlowProcess<Properties> process, Tap tap) {
        if (schema == null) {
            // no need to open them all
            if (tap instanceof CompositeTap)
                tap = (Tap) ((CompositeTap) tap).getChildTaps().next();

            final String path = tap.getIdentifier();
            File f = new File(path);
            List<File> secondaryFiles = new ArrayList<File>();
            if (f.isFile()) {
                try {
                    @SuppressWarnings("unchecked") DataFileReader reader = new DataFileReader(f, new GenericDatumReader());
                    schema = reader.getSchema();
                }
                catch (IOException e) {
                    LOG.info("Couldn't open " + f.toString(), e);
                }
            }
            if (f.listFiles() != null)
                for (File file : f.listFiles()) {
                    if (file.isFile()) {
                        try {
                            @SuppressWarnings("unchecked") DataFileReader reader = new DataFileReader(file, new GenericDatumReader());
                            schema = reader.getSchema();
                            break;
                        }
                        catch (IOException e) {
                            LOG.info("Couldn't open " + file.toString(), e);
                        }
                    }
                    else if (file.isDirectory()) secondaryFiles.addAll(Arrays.asList(file.listFiles()));
                }
            if (schema == null) {
                for (File file : secondaryFiles) {
                    if (file.isFile()) {
                        try {
                            @SuppressWarnings("unchecked") DataFileReader reader = new DataFileReader(file, new GenericDatumReader());
                            schema = reader.getSchema();
                            break;
                        }
                        catch (IOException e) {
                            LOG.info("Couldn't open " + file.toString(), e);
                        }
                    }
                }
            }
            if (schema == null) {
                LOG.info("Couldn't find any concrete Schema. Using Schema.Type.NULL. Things will probably break.");
                schema = Schema.create(Schema.Type.NULL);
            }
        }
        Fields cascadingFields = new Fields();
        if (schema.getType().equals(Schema.Type.NULL)) {
            cascadingFields = Fields.NONE;
        }
        else {
            for (Schema.Field avroField : schema.getFields())
                cascadingFields = cascadingFields.append(new Fields(avroField.name()));
        }
        setSourceFields(cascadingFields);
        return getSourceFields();
    }

    /**
     * Do nothing.
     *
     * @param process
     * @param tap
     * @param fields
     */
    @Override
    public void presentSourceFields(FlowProcess<Properties> process, Tap tap, Fields fields) {
    }

    /**
     * Do nothing.
     *
     * @param flowProcess
     * @param tap
     * @param fields
     */
    @Override
    public void presentSinkFields(FlowProcess<Properties> flowProcess, Tap tap, Fields fields) {
    }

    /**
     * Do nothing.
     *
     * @param flowProcess
     * @param tap
     * @param conf
     */
    @Override
    public void sourceConfInit(FlowProcess<Properties> flowProcess, Tap<Properties, InputStream, OutputStream> tap, Properties conf) {
    }

    /**
     * Method sourcePrepare will open up a DataFileStream via the {@link AvroScheme#createInput(java.io.InputStream)} method.
     *
     * @param flowProcess
     * @param sourceCall
     * @throws IOException
     */
    @Override
    public void sourcePrepare(FlowProcess<Properties> flowProcess, SourceCall<DataFileStream, InputStream> sourceCall) throws IOException {
        sourceCall.setContext(createInput(sourceCall.getInput()));
    }


    /**
     * Method source will read a new "record" or value from {@link cascading.scheme.SourceCall#getInput()} and populate
     * the available {@link cascading.tuple.Tuple} via {@link cascading.scheme.SourceCall#getIncomingEntry()} and return {@code true}
     * on success or {@code false} if no more values available.
     * <p/>
     * It's ok to set a new Tuple instance on the {@code incomingEntry} {@link cascading.tuple.TupleEntry}, or
     * to simply re-use the existing instance.
     * <p/>
     * Note this is only time it is safe to modify a Tuple instance handed over via a method call.
     * <p/>
     * This method may optionally throw a {@link cascading.tap.TapException} if it cannot process a particular
     * instance of data. If the payload Tuple is set on the TapException, that Tuple will be written to
     * any applicable failure trap Tap.
     *
     * @param flowProcess of type FlowProcess
     * @param sourceCall  of SourceCall
     * @return returns {@code true} when a Tuple was successfully read
     */
    @Override
    public boolean source(FlowProcess<Properties> flowProcess, SourceCall<DataFileStream, InputStream> sourceCall) throws IOException {

        if (sourceCall.getContext().hasNext()) {
            IndexedRecord record = (IndexedRecord) sourceCall.getContext().next();
            this.avroToCascading.convertRecord(sourceCall.getIncomingEntry(), record, schema, this.recordUnpackDepth);
            return true;
        }
        else
            return false;
    }

    /**
     * Method sourceCleanup will close the DataFileStream for further reading.
     *
     * @param flowProcess
     * @param sourceCall
     * @throws IOException
     */
    @Override
    public void sourceCleanup(FlowProcess<Properties> flowProcess, SourceCall<DataFileStream, InputStream> sourceCall) throws IOException {
        sourceCall.getContext().close();
    }

    /**
     * Do Nothing
     *
     * @param flowProcess
     * @param tap
     * @param conf
     */
    @Override
    public void sinkConfInit(FlowProcess<Properties> flowProcess, Tap<Properties, InputStream, OutputStream> tap, Properties conf) {
    }


    /**
     * Method sinkPrepare will open up the DataFileWriter.
     *
     * @param flowProcess
     * @param sinkCall
     */
    @Override
    public void sinkPrepare(FlowProcess<Properties> flowProcess, SinkCall<DataFileWriter, OutputStream> sinkCall) {
        if (schema == null) throw new RuntimeException("Cannot have a null schema for the sink (yet).");
        sinkCall.setContext(createOutput(sinkCall.getOutput()));
    }

    /**
     * Method sink writes out the given {@link cascading.tuple.Tuple} found on {@link cascading.scheme.SinkCall#getOutgoingEntry()} to
     * the {@link cascading.scheme.SinkCall#getOutput()}.
     * <p/>
     * This method may optionally throw a {@link cascading.tap.TapException} if it cannot process a particular
     * instance of data. If the payload Tuple is set on the TapException, that Tuple will be written to
     * any applicable failure trap Tap. If not set, the incoming Tuple will be written instead.
     *
     * @param flowProcess of Process
     * @param sinkCall    of SinkCall
     */
    @Override
    public void sink(FlowProcess<Properties> flowProcess, SinkCall<DataFileWriter, OutputStream> sinkCall) throws IOException {
        TupleEntry tupleEntry = sinkCall.getOutgoingEntry();

        IndexedRecord record = this.cascadingToAvro.convertRecord(tupleEntry, schema, this.recordUnpackDepth);

        //noinspection unchecked
        sinkCall.getContext().append(record);
    }

    protected Function<Schema, IndexedRecord> avroRecordBuilder() {
        return new Function<Schema, IndexedRecord>() {
            @Override
            public IndexedRecord apply(Schema schema) {
                return new GenericData.Record(schema);
            }
        };
    }

    /**
     * Method sinkCleanup will flush the DataFileWriter and close the file. It will result in a runtime exception if this
     * either flush or close throw an exception.
     *
     * @param flowProcess
     * @param sinkCall
     */
    @Override
    public void sinkCleanup(FlowProcess<Properties> flowProcess, SinkCall<DataFileWriter, OutputStream> sinkCall) {
        try {
            sinkCall.getContext().flush();
            sinkCall.getContext().close();
        }
        catch (IOException e) {
            LOG.error("Unable to flush and close the output sink.");
            e.printStackTrace();
            System.exit(1);
        }
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
            schema.hashCode();
    }
}

