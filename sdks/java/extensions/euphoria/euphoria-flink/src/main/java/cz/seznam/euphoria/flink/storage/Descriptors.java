package cz.seznam.euphoria.flink.storage;

import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;

/** Helper around storage descriptors. */
public class Descriptors {

  public static <T> ReducingStateDescriptor<T>
  from(ValueStorageDescriptor.MergingValueStorageDescriptor<T> descriptor) {
    return new ReducingStateDescriptor<T>(
        descriptor.getName(),
        new ReducingMerger<>(descriptor.getValueMerger()),
        descriptor.getValueClass());
  }

  /** Converts the given euphoria descriptor into its flink equivalent. */
  public static <T> ValueStateDescriptor<T> from(ValueStorageDescriptor<T> descriptor) {
    return new ValueStateDescriptor<>(
        descriptor.getName(),
        descriptor.getValueClass(),
        descriptor.getDefaultValue());
  }

  /** Converts the given euphoria descriptor into its flink equivalent. */
  public static <T> ListStateDescriptor<T> from(ListStorageDescriptor<T> descriptor) {
    return new ListStateDescriptor<>(
        descriptor.getName(),
        descriptor.getElementClass());
  }

  private Descriptors(){}
}