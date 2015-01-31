package cascading.avro.conversion;

import com.google.common.collect.ImmutableSet;
import org.apache.avro.Schema;

import java.io.Serializable;

/**
 * Defines a mapping from Java classes to Avro Schema.Type's.
 *
 * Should provide a parameter-less constructor for instantiation after deserialization
 */
public interface TypeMapping {
  ImmutableSet<Schema.Type> getDestinationTypes(Class<?> c);

  boolean isMappable(Class<?> c, Schema.Type t);
}
