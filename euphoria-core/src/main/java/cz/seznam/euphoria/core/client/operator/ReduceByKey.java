/**
 * Copyright 2016 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.annotation.operator.Recommended;
import cz.seznam.euphoria.core.annotation.operator.StateComplexity;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.ReduceFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.util.Pair;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Objects;

/**
 * Operator performing state-less aggregation by given reduce function.
 *
 * @param <IN> Type of input records
 * @param <KIN> Type of records entering #keyBy and #valueBy methods
 * @param <KEY> Output type of #keyBy method
 * @param <VALUE> Output type of #valueBy method
 * @param <KEYOUT> Type of output key
 * @param <OUT> Type of output value
 */
@Recommended(
    reason =
        "Is very recommended to override because of performance in "
      + "a specific area of (mostly) batch calculations where combiners "
      + "can be efficiently used in the executor-specific implementation",
    state = StateComplexity.CONSTANT_IF_COMBINABLE,
    repartitions = 1
)
public class ReduceByKey<
    IN, KIN, KEY, VALUE, KEYOUT, OUT, W extends Window>
    extends StateAwareWindowWiseSingleInputOperator<
        IN, IN, KIN, KEY, Pair<KEYOUT, OUT>, W,
        ReduceByKey<IN, KIN, KEY, VALUE, KEYOUT, OUT, W>> {

  public static class OfBuilder {
    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    public <IN> DatasetBuilder1<IN> of(Dataset<IN> input) {
      return new DatasetBuilder1<>(name, input);
    }
  }

  // builder classes used when input is Dataset<IN> ----------------------

  public static class DatasetBuilder1<IN> {
    private final String name;
    private final Dataset<IN> input;
    DatasetBuilder1(String name, Dataset<IN> input) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
    }
    public <KEY> DatasetBuilder2<IN, KEY> keyBy(UnaryFunction<IN, KEY> keyExtractor) {
      return new DatasetBuilder2<>(name, input, keyExtractor);
    }
  }

  public static class DatasetBuilder2<IN, KEY> {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, KEY> keyExtractor;
    DatasetBuilder2(String name, Dataset<IN> input, UnaryFunction<IN, KEY> keyExtractor) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
    }
    public <VALUE> DatasetBuilder3<IN, KEY, VALUE> valueBy(UnaryFunction<IN, VALUE> valueExtractor) {
      return new DatasetBuilder3<>(name, input, keyExtractor, valueExtractor);
    }
    public <OUT> DatasetBuilder4<IN, KEY, IN, OUT> reduceBy(ReduceFunction<IN, OUT> reducer) {
      return new DatasetBuilder4<>(name, input, keyExtractor, e-> e, reducer);
    }
    @SuppressWarnings("unchecked")
    public DatasetBuilder4<IN, KEY, IN, IN> combineBy(CombinableReduceFunction<IN> reducer) {
      return new DatasetBuilder4(name, input, keyExtractor, e -> e, (ReduceFunction) reducer);
    }
  }
  public static class DatasetBuilder3<IN, KEY, VALUE> {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, KEY> keyExtractor;
    private final UnaryFunction<IN, VALUE> valueExtractor;
    DatasetBuilder3(String name,
                    Dataset<IN> input,
                    UnaryFunction<IN, KEY> keyExtractor,
                    UnaryFunction<IN, VALUE> valueExtractor) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.valueExtractor = Objects.requireNonNull(valueExtractor);
    }
    public <OUT> DatasetBuilder4<IN, KEY, VALUE, OUT> reduceBy(
        ReduceFunction<VALUE, OUT> reducer) {
      return new DatasetBuilder4<>(name, input, keyExtractor, valueExtractor, reducer);
    }
    public DatasetBuilder4<IN, KEY, VALUE, VALUE> combineBy(
        CombinableReduceFunction<VALUE> reducer) {
      return new DatasetBuilder4<>(name, input, keyExtractor, valueExtractor, reducer);
    }
  }
  public static class DatasetBuilder4<IN, KEY, VALUE, OUT>
          extends PartitioningBuilder<KEY, DatasetBuilder4<IN, KEY, VALUE, OUT>>
          implements OutputBuilder<Pair<KEY, OUT>> {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, KEY> keyExtractor;
    private final UnaryFunction<IN, VALUE> valueExtractor;
    private final ReduceFunction<VALUE, OUT> reducer;
    DatasetBuilder4(String name,
                    Dataset<IN> input,
                    UnaryFunction<IN, KEY> keyExtractor,
                    UnaryFunction<IN, VALUE> valueExtractor,
                    ReduceFunction<VALUE, OUT> reducer) {

      // initialize default partitioning according to input
      super(new DefaultPartitioning<>(input.getNumPartitions()));

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.valueExtractor = Objects.requireNonNull(valueExtractor);
      this.reducer = Objects.requireNonNull(reducer);
    }
    public  <W extends Window>
    DatasetBuilder5<IN, KEY, VALUE, OUT, W>
    windowBy(Windowing<IN, W> windowing) {
      return windowBy(windowing, null);
    }
    public  <W extends Window>
    DatasetBuilder5<IN, KEY, VALUE, OUT, W>
    windowBy(Windowing<IN, W> windowing, UnaryFunction<IN, Long> eventTimeAssigner) {
      return new DatasetBuilder5<>(name, input, keyExtractor, valueExtractor,
              reducer, Objects.requireNonNull(windowing), eventTimeAssigner, this);
    }

    @Override
    public Dataset<Pair<KEY, OUT>> output() {
      return new DatasetBuilder5<>(name, input, keyExtractor, valueExtractor,
              reducer, null, null, this)
          .output();
    }
  }

  public static class DatasetBuilder5<
          IN, KEY, VALUE, OUT, W extends Window>
      extends PartitioningBuilder<KEY, DatasetBuilder5<IN, KEY, VALUE, OUT, W>>
      implements OutputBuilder<Pair<KEY, OUT>> {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, KEY> keyExtractor;
    private final UnaryFunction<IN, VALUE> valueExtractor;
    private final ReduceFunction<VALUE, OUT> reducer;
    @Nullable
    private final Windowing<IN, W> windowing;
    @Nullable
    private final UnaryFunction<IN, Long> eventTimeAssigner;

    DatasetBuilder5(String name,
                    Dataset<IN> input,
                    UnaryFunction<IN, KEY> keyExtractor,
                    UnaryFunction<IN, VALUE> valueExtractor,
                    ReduceFunction<VALUE, OUT> reducer,
                    @Nullable Windowing<IN, W> windowing,
                    @Nullable UnaryFunction<IN, Long> eventTimeAssigner,
                    PartitioningBuilder<KEY, ?> partitioning) {

      // initialize default partitioning according to input
      super(partitioning);

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.valueExtractor = Objects.requireNonNull(valueExtractor);
      this.reducer = Objects.requireNonNull(reducer);
      this.windowing = windowing;
      this.eventTimeAssigner = eventTimeAssigner;
    }

    @Override
    public Dataset<Pair<KEY, OUT>> output() {
      Flow flow = input.getFlow();
      ReduceByKey<IN, IN, KEY, VALUE, KEY, OUT, W>
          reduce =
          new ReduceByKey<>(name, flow, input, keyExtractor, valueExtractor,
              windowing, eventTimeAssigner, reducer, getPartitioning());
      flow.add(reduce);
      return reduce.output();
    }
  }

  public static <IN> DatasetBuilder1<IN> of(Dataset<IN> input) {
    return new DatasetBuilder1<>("ReduceByKey", input);
  }

  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  final ReduceFunction<VALUE, OUT> reducer;
  final UnaryFunction<KIN, VALUE> valueExtractor;

  ReduceByKey(String name,
              Flow flow,
              Dataset<IN> input,
              UnaryFunction<KIN, KEY> keyExtractor,
              UnaryFunction<KIN, VALUE> valueExtractor,
              @Nullable Windowing<IN, W> windowing,
              @Nullable UnaryFunction<IN, Long> eventTimeAssigner,
              ReduceFunction<VALUE, OUT> reducer,
              Partitioning<KEY> partitioning) {
    super(name, flow, input, keyExtractor, windowing, eventTimeAssigner, partitioning);
    this.reducer = reducer;
    this.valueExtractor = valueExtractor;
  }

  public ReduceFunction<VALUE, OUT> getReducer() {
    return reducer;
  }

  public UnaryFunction<KIN, VALUE> getValueExtractor() {
    return valueExtractor;
  }

  /**
   * @return {@code TRUE} when combinable reduce function provided
   */
  public boolean isCombinable() {
    return reducer instanceof CombinableReduceFunction;
  }

  // state represents the output value
  private static class ReduceState<VALUE, OUT> extends State<VALUE, OUT> {

    private final ReduceFunction<VALUE, OUT> reducer;
    private final boolean combinable;

    final ListStorage<VALUE> reducableValues;

    ReduceState(Context<OUT> context,
                StorageProvider storageProvider,
                ReduceFunction<VALUE, OUT> reducer,
                boolean combinable) {
      super(context, storageProvider);
      this.reducer = Objects.requireNonNull(reducer);
      this.combinable = combinable;
      @SuppressWarnings("unchecked")
      ListStorageDescriptor<VALUE> values =
          ListStorageDescriptor.of("values", (Class) Object.class);
      reducableValues = storageProvider.getListStorage(values);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void add(VALUE element) {
      reducableValues.add(element);
      combineIfPossible();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void flush() {
      OUT result = reducer.apply(reducableValues.get());
      getContext().collect(result);
    }

    void add(ReduceState<VALUE, OUT> other) {
      this.reducableValues.addAll(other.reducableValues.get());
      combineIfPossible();
    }

    @SuppressWarnings("unchecked")
    private void combineIfPossible() {
      if (combinable) {
        OUT val = reducer.apply(reducableValues.get());
        reducableValues.clear();
        reducableValues.add((VALUE) val);
      }
    }

    @Override
    public void close() {
      reducableValues.clear();
    }

  }

  @SuppressWarnings("unchecked")
  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    // this can be implemented using ReduceStateByKey

    Flow flow = getFlow();
    Operator<?, ?> reduceState;
    reduceState = new ReduceStateByKey<>(getName(),
        flow, input, keyExtractor, valueExtractor,
        windowing,
        eventTimeAssigner,
        (Context<OUT> c, StorageProvider provider) -> new ReduceState<>(
            c, provider, reducer, isCombinable()),
        (Iterable<ReduceState> states) -> {
          final ReduceState first;
          Iterator<ReduceState> i = states.iterator();
          first = i.next();
          while (i.hasNext()) {
            first.add(i.next());
          }
          return first;
        },
        partitioning);
    return DAG.of(reduceState);
  }
}
