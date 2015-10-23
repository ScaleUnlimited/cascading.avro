package cascading.avro.local;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

public class PackedAvroScheme<T> extends AvroScheme {
    private static final Logger LOG = LoggerFactory.getLogger(PackedAvroScheme.class);

    /**
     * This scheme should be used when you don't want cascading.avro to automatically unpack or pack your Avro objects.
     * The constructors are similar to the super class but there is only ever one field incoming or outgoing. The parameter
     * is the type of Avro record to read.
     */


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
        setSinkFields(Fields.FIRST);
        setSourceFields(Fields.FIRST);
    }

    @Override
    DataFileStream createInput(InputStream inputStream) {
        DatumReader<T> datumReader = new SpecificDatumReader<T>(schema);
        try {
            return new DataFileStream<T>(inputStream, datumReader);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        // should never reach here
        return null;
    }

    @Override
    DataFileWriter createOutput(OutputStream outputStream) {
        Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

        DatumWriter<T> datumWriter = new SpecificDatumWriter<T>(schema);

        DataFileWriter<T> dataWriter = new DataFileWriter<T>(datumWriter);

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
     * Sink method to take an outgoing tuple and write it to Avro. In this scheme the incoming avro is passed through.
     *
     * @param flowProcess The cascading FlowProcess object. Should be passed in by cascading automatically.
     * @param sinkCall    The cascading SinkCall object. Should be passed in by cascading automatically.
     * @throws java.io.IOException
     */
    @Override
    public void sink(FlowProcess<? extends Properties> flowProcess, SinkCall<DataFileWriter, OutputStream> sinkCall) throws IOException {
        TupleEntry tupleEntry = sinkCall.getOutgoingEntry();
        //noinspection unchecked
        sinkCall.getContext().append((T) tupleEntry.getObject(Fields.FIRST));
    }


    /**
     * Sets to Fields.UNKNOWN if no schema is present, otherwise uses the name of the Schema.
     *
     * @param flowProcess The cascading FlowProcess object. Should be passed in by cascading automatically.
     * @param tap         The cascading Tap object. Should be passed in by cascading automatically.
     * @return Fields The source cascading fields.
     */
    @Override
    public Fields retrieveSourceFields(FlowProcess<? extends Properties> flowProcess, Tap tap) {
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
    public boolean source(FlowProcess<? extends Properties> flowProcess, SourceCall<DataFileStream, InputStream> sourceCall) {
        if (sourceCall.getContext().hasNext()) {
            T record = (T) sourceCall.getContext().next();
            Tuple tuple = sourceCall.getIncomingEntry().getTuple();
            tuple.clear();
            tuple.add(record);
            return true;
        }
        return false;
    }

}
