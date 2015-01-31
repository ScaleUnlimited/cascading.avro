package cascading.avro;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroJob;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.trevni.avro.AvroTrevniInputFormat;
import org.apache.trevni.avro.AvroTrevniOutputFormat;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;

public class TrevniScheme extends AvroScheme {
	
	public TrevniScheme(Schema schema)
	{
		super(schema);
	}
	
	@Override
	public void sourceConfInit(FlowProcess<JobConf> flowProcess,
			Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {

		retrieveSourceFields(flowProcess, tap);
		
		// Set the input schema and input format class
		conf.set(AvroJob.INPUT_SCHEMA, schema.toString());	
		conf.setInputFormat(AvroTrevniInputFormat.class);

		// add AvroSerialization to io.serializations
		addAvroSerializations(conf);	
	}

	@Override
	public void sinkConfInit(FlowProcess<JobConf> flowProcess,
			Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {

		if (schema == null) {
			throw new RuntimeException("Must provide sink schema");
		}
		
		// Set the output schema and output format class
		conf.set(AvroJob.OUTPUT_SCHEMA, schema.toString());
		conf.setOutputFormat(AvroTrevniOutputFormat.class);

		// add AvroSerialization to io.serializations
		addAvroSerializations(conf);
	}
}
