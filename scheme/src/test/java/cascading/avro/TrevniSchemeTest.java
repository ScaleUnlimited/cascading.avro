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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

public class TrevniSchemeTest extends Assert {

	@Rule
	public final TemporaryFolder tempDir = new TemporaryFolder();
	
	
	@Test
	public void testSpecifiedColumns() throws Exception {
		
		final Schema schema = new Schema.Parser().parse(getClass()
				.getResourceAsStream("electric-power-usage.avsc"));
		
		
		final Schema specifiedColumnsSchema = new Schema.Parser().parse(getClass()
				.getResourceAsStream("electric-power-usage2.avsc"));
		
		Configuration hadoopConf = new Configuration();	
		
		// compression codec for trevni column block.
		// KKr - This fails on systems without Snappy installed, so commenting it out
		// hadoopConf.set("trevni.meta.trevni.codec", "snappy");
		
		Map<Object, Object> confMap = new HashMap<Object, Object>();
		Iterator<Entry<String, String>> iter = hadoopConf.iterator();
		while (iter.hasNext()) {
			Entry<String, String> entry = iter.next();
			confMap.put(entry.getKey(), entry.getValue());
		}
		
		JobConf jobConf = new JobConf(hadoopConf);		
		
		String in = tempDir.getRoot().toString() + "/specifiedColumns/in";
		String out = tempDir.getRoot().toString() + "/specifiedColumns/out";	

		final Fields fields = new Fields("addressCode", "timestamp", "devicePowerEventList");
		
		final Fields innerFields = new Fields("power", "deviceType", "deviceId", "status");
		
		
		Tap lfsSource = new Lfs(new TrevniScheme(schema), in, SinkMode.REPLACE);
		
		TupleEntryCollector write = lfsSource.openForWrite(new HadoopFlowProcess(jobConf));
		
		List<TupleEntry> devicePowerEventList = new ArrayList<TupleEntry>();
		devicePowerEventList.add(new TupleEntry(innerFields, new Tuple(1300.0, 5, 0, 1)));
		devicePowerEventList.add(new TupleEntry(innerFields, new Tuple(3500.4, 4, 1, 0)));
		
		List<TupleEntry> devicePowerEventList2 = new ArrayList<TupleEntry>();
		devicePowerEventList2.add(new TupleEntry(innerFields, new Tuple(3570.0, 3, 0, 1)));
		devicePowerEventList2.add(new TupleEntry(innerFields, new Tuple(110.4, 2, 1, 0)));
		devicePowerEventList2.add(new TupleEntry(innerFields, new Tuple(250.9, 3, 3, 1)));

		
		write.add(new TupleEntry(fields, new Tuple("4874025000-514", 1356998460000L, devicePowerEventList)));
		write.add(new TupleEntry(fields, new Tuple("4725033000-4031", 1356998520000L, devicePowerEventList2)));
	
		write.close();

		Pipe writePipe = new Pipe("tuples to trevni");
		Tap lfsTrevniSource = new Lfs(new TrevniScheme(schema), in + "/*");
		Tap trevniSink = new Lfs(new TrevniScheme(schema), out);

		Flow flow = new HadoopFlowConnector(confMap).connect(lfsTrevniSource, trevniSink, writePipe);
		flow.complete();

		// Read the specified columns.		
		Tap trevniSource = new Lfs(new TrevniScheme(specifiedColumnsSchema), out + "/*");	

		TupleEntryIterator iterator = trevniSource.openForRead(new HadoopFlowProcess(jobConf));

		assertTrue(iterator.hasNext());
		
		final TupleEntry readEntry1 = iterator.next();
		
		assertTrue(readEntry1.getString("addressCode").equals("4874025000-514"));
		Tuple devicePowerEventList1 = (Tuple) readEntry1.getObject("devicePowerEventList");
		assertEquals(2, devicePowerEventList1.size());
		assertEquals(1300.0, ((TupleEntry) devicePowerEventList1.getObject(0)).getDouble(0));
		
		final TupleEntry readEntry2 = iterator.next();
		
		assertTrue(readEntry2.getString("addressCode").equals("4725033000-4031"));
		assertEquals(3, ((Tuple)readEntry2.getObject("devicePowerEventList")).size());
		assertEquals(110.4, ((TupleEntry) ((Tuple) readEntry1.getObject("devicePowerEventList")).getObject(1)).getDouble(0));
	}
}
