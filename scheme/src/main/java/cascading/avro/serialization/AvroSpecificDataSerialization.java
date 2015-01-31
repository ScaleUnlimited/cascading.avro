package cascading.avro.serialization;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * This class provides a serialization for all Avro-generated Java classes served by SpecificData.
 * <p/>
 * This allows for serialization of Avro objects embedded in cascading tuples during intermediate serialization such
 * as during a reduce phase. Add it to the list with the other serialization classes in the JobConf "io.serializations"
 * property. The order is important; place this serializer earlier in the list than those serializations you wish to override.
 *
 * @param <T> The type to serialize/deserialize.
 */
public class AvroSpecificDataSerialization<T> implements Serialization<T> {
  @Override
  public boolean accept(Class<?> c) {
    try {
      // All SpecificData will provide a serialization for Longs etc, prefer the default Cascading serialization for
      // these types.
      return (GenericContainer.class.isAssignableFrom(c) || Enum.class.isAssignableFrom(c)) &&
          getSchema(c) != null;
    }
    catch (AvroRuntimeException e) {
      return false;
    }
  }

  @Override
  public Serializer<T> getSerializer(Class<T> c) {
    return new AvroSpecificDataSerializer();
  }

  @Override
  public Deserializer<T> getDeserializer(Class<T> c) {
    return new AvroSpecificDataDeserializer(c);
  }

  public Schema getSchema(T t) {
    return getSchema(t.getClass());
  }

  protected Schema getSchema(Class<?> clazz) {
    return SpecificData.get().getSchema(clazz);
  }

  class AvroSpecificDataSerializer implements Serializer<T> {
    private DatumWriter<T> writer;
    private BinaryEncoder encoder;
    private OutputStream outStream;

    AvroSpecificDataSerializer() {
      // Schema set before write on each call to Serialize
      this.writer = new SpecificDatumWriter<T>();
    }

    @Override
    public void close() throws IOException {
      outStream.close();
    }

    @Override
    public void open(OutputStream out) throws IOException {
      outStream = out;
      // Note that we must not buffer output (in case we fail to flush a record between writes to the OutputStream
      // by other serializers), in accordance with the Serializer<T> interface contract. Specifically the comment:
      // "Serializers are stateful, but must not buffer the output...". So we use a directBinaryEncoder.
      encoder = EncoderFactory.get().directBinaryEncoder(out, encoder);
    }

    @Override
    public void serialize(T t) throws IOException {
      writer.setSchema(getSchema(t));
      writer.write(t, encoder);
    }
  }

  class AvroSpecificDataDeserializer implements Deserializer<T> {
    private DatumReader<T> reader;
    private BinaryDecoder decoder;
    private InputStream inStream;

    AvroSpecificDataDeserializer(Class<T> clazz) {
      this.reader = new SpecificDatumReader<T>(getSchema(clazz));
    }

    @Override
    public void close() throws IOException {
      inStream.close();
    }

    @Override
    public T deserialize(T t) throws IOException {
      return reader.read(t, decoder);
    }

    @Override
    public void open(InputStream in) throws IOException {
      inStream = in;
      // Note that we must not buffer input (in case we read ahead into serialized data belonging to a different
      // serialization), in accordance with the Serializer<T> interface contract. Specifically the comment:
      // "Deserializers are stateful, but must not buffer the input...". So we use a directBinaryDecoder.
      decoder = DecoderFactory.get().directBinaryDecoder(in, decoder);
    }
  }
}
