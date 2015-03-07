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

package cascading.avro.conversion;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.io.BytesWritable;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class AvroToCascading extends AvroConverterBase<IndexedRecord, TupleEntry> {
    @Override
    public TupleEntry convertRecord(IndexedRecord record, Schema schema, int toDepth) {
        if (toDepth == 0)
            throw new IllegalArgumentException("Conversion tree too deep");
        Fields fields = new Fields();
        for (Field recordField : record.getSchema().getFields()) {
            fields = fields.append(new Fields(recordField.name()));
        }
        TupleEntry tupleEntry = new TupleEntry(fields, new Tuple(record.getSchema().getFields().size()));
        return convertRecord(tupleEntry, record, schema, toDepth);
    }

    @Override
    public TupleEntry convertRecord(TupleEntry tupleEntry, IndexedRecord record, Schema readerSchema, int toDepth) {
        convertRecord(tupleEntry.getTuple(), record, readerSchema, toDepth);
        return tupleEntry;
    }

    public Tuple convertRecord(Tuple tuple, IndexedRecord record, Schema readerSchema, int toDepth) {
        tuple.clear();
        Schema writerSchema = record.getSchema();
        for (Field field : readerSchema.getFields()) {
            if (writerSchema.getField(field.name()) == null) {
                throw new AvroRuntimeException("Not a valid schema field: " + field.name());
            }
            tuple.add(convert(record.get(field.pos()), field.schema(), toDepth - 1));
        }
        return tuple;
    }

    @Override
    protected boolean isRecordToType(Object obj) {
        return obj instanceof TupleEntry;
    }

    @Override
    public Object convertFixed(Object obj, Schema schema) {
        GenericData.Fixed fixed = (GenericData.Fixed) obj;
        return new BytesWritable(fixed.bytes());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object convertMap(Object obj, Schema schema, int toDepth) {
        Tuple tuple = new Tuple();
        // CharSequence because the str can be configured as either Utf8 or String.
        for (Map.Entry<CharSequence, Object> e : ((Map<CharSequence, Object>) obj).entrySet()) {
            tuple.add(e.getKey().toString());
            tuple.add(convert(e.getValue(), schema.getValueType(), toDepth));
        }
        return tuple;
    }

    @Override
    public BytesWritable convertBytes(Object obj) {
        byte[] bytes = obj instanceof byte[] ? (byte[]) obj : ((ByteBuffer) obj).array();
        return new BytesWritable(bytes);
    }

    @Override
    public Object convertArray(Object obj, Schema schema, int toDepth) {
        Tuple tuple = new Tuple();
        for (Object element : (Iterable) obj) {
            tuple.add(convert(element, schema.getElementType(), toDepth));
        }
        return tuple;
    }

    @Override
    public Object convertUnion(Object obj, Schema schema, int toDepth) {
        List<Schema> types = schema.getTypes();
        int i = SpecificData.get().resolveUnion(schema, obj);
        return convert(obj, types.get(i), toDepth);
    }
}
