package cascading.avro.local;

import cascading.avro.AvroToCascading;
import cascading.avro.CascadingToAvro;
import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.CompositeTap;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
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
    private Encoder encoder;
    //    private Decoder decoder;
    protected Schema schema;
    private GenericDatumReader<IndexedRecord> datumReader;
    private GenericDatumWriter<IndexedRecord> datumWriter;

    /**
     * Constructor that takes an Avro Schema. It will create the incoming and outgoing fields from the Schema if it isn't
     * null. If it is null assume the sink is Fields.ALL and the source is Fields.UNKNOWN. These will be set later.
     *
     * @param schema
     */
    public AvroScheme(Schema schema) {
        this.schema = schema;
        if (schema == null) {
            setSinkFields(Fields.ALL);
            setSourceFields(Fields.UNKNOWN);
        } else {
            Fields cascadingFields = new Fields();
            for (Schema.Field avroField : schema.getFields()) {
                cascadingFields = cascadingFields.append(
                        new Fields(avroField.name()));
            }
            setSinkFields(cascadingFields);
            setSourceFields(cascadingFields);
        }
    }

    /**
     * Pass through to the Schema constructor with a null schema.
     */
    public AvroScheme() {
        this(null);
    }

    DataFileStream<IndexedRecord> createInput(InputStream inputStream) {
//        if (decoder == null)
//            decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        if (datumReader == null)
            datumReader = new GenericDatumReader<IndexedRecord>(schema);
        try {
            return new DataFileStream<IndexedRecord>(inputStream, datumReader);
        } catch (IOException e) {
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
        } catch (IOException e) {
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
    public Fields retrieveSourceFields(FlowProcess<? extends Properties> process, Tap tap) {
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
                } catch (IOException e) {
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
                        } catch (IOException e) {
                            LOG.info("Couldn't open " + file.toString(), e);
                        }
                    } else if (file.isDirectory()) secondaryFiles.addAll(Arrays.asList(file.listFiles()));
                }
            if (schema == null) {
                for (File file : secondaryFiles) {
                    if (file.isFile()) {
                        try {
                            @SuppressWarnings("unchecked") DataFileReader reader = new DataFileReader(file, new GenericDatumReader());
                            schema = reader.getSchema();
                            break;
                        } catch (IOException e) {
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
        } else {
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
    public void presentSourceFields(FlowProcess<? extends Properties> process, Tap tap, Fields fields) {
    }

    /**
     * Do nothing.
     *
     * @param flowProcess
     * @param tap
     * @param fields
     */
    @Override
    public void presentSinkFields(FlowProcess<? extends Properties> flowProcess, Tap tap, Fields fields) {
    }

    /**
     * Do nothing.
     *
     * @param flowProcess
     * @param tap
     * @param conf
     */
    @Override
    public void sourceConfInit(FlowProcess<? extends Properties> flowProcess, Tap<Properties, InputStream, OutputStream> tap, Properties conf) {
    }

    /**
     * Method sourcePrepare will open up a DataFileStream via the {@link AvroScheme#createInput(java.io.InputStream)} method.
     *
     * @param flowProcess
     * @param sourceCall
     * @throws IOException
     */
    @Override
    public void sourcePrepare(FlowProcess<? extends Properties> flowProcess, SourceCall<DataFileStream, InputStream> sourceCall) throws IOException {
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
    public boolean source(FlowProcess<? extends Properties> flowProcess, SourceCall<DataFileStream, InputStream> sourceCall) throws IOException {

        if (sourceCall.getContext().hasNext()) {
            IndexedRecord record = (IndexedRecord) sourceCall.getContext().next();
            Tuple tuple = sourceCall.getIncomingEntry().getTuple();
            tuple.clear();
            Object[] split = AvroToCascading.parseRecord(record, schema);
            tuple.addAll(split);
            return true;
        } else
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
    public void sourceCleanup(FlowProcess<? extends Properties> flowProcess, SourceCall<DataFileStream, InputStream> sourceCall) throws IOException {
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
    public void sinkConfInit(FlowProcess<? extends Properties> flowProcess, Tap<Properties, InputStream, OutputStream> tap, Properties conf) {
    }


    /**
     * Method sinkPrepare will open up the DataFileWriter.
     *
     * @param flowProcess
     * @param sinkCall
     */
    @Override
    public void sinkPrepare(FlowProcess<? extends Properties> flowProcess, SinkCall<DataFileWriter, OutputStream> sinkCall) {
        if (schema == null)
            throw new RuntimeException("Cannot have a null schema for the sink (yet).");

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
    public void sink(FlowProcess<? extends Properties> flowProcess, SinkCall<DataFileWriter, OutputStream> sinkCall) throws IOException {
        TupleEntry tupleEntry = sinkCall.getOutgoingEntry();

        IndexedRecord record = new GenericData.Record(schema);
        Object[] objectArray = CascadingToAvro.parseTupleEntry(tupleEntry, schema);
        for (int i = 0; i < objectArray.length; i++) {
            record.put(i, objectArray[i]);
        }

        sinkCall.getContext().append(record);


    }

    /**
     * Method sinkCleanup will flush the DataFileWriter and close the file. It will result in a runtime exception if this
     * either flush or close throw an exception.
     *
     * @param flowProcess
     * @param sinkCall
     */
    @Override
    public void sinkCleanup(FlowProcess<? extends Properties> flowProcess, SinkCall<DataFileWriter, OutputStream> sinkCall) {
        try {
            sinkCall.getContext().flush();
            sinkCall.getContext().close();
        } catch (IOException e) {
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
        return 31 * (getSinkFields() == null ? 0 : getSinkFields().hashCode()) +
                (schema == null ? 0 : schema.hashCode());
    }
}

