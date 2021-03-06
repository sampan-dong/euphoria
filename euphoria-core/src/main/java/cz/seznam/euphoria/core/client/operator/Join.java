/*
 * Copyright 2016-2018 Seznam.cz, a.s.
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

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.annotation.operator.Recommended;
import cz.seznam.euphoria.core.annotation.operator.StateComplexity;
import cz.seznam.euphoria.core.annotation.stability.Experimental;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.BinaryFunctor;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.operator.hint.OutputHint;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StateContext;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.util.Either;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.graph.DAG;
import cz.seznam.euphoria.shadow.com.google.common.annotations.VisibleForTesting;
import cz.seznam.euphoria.shadow.com.google.common.collect.Sets;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;

/**
 * Inner join of two datasets by given key producing single new dataset.
 *
 * When joining two streams, the join has to specify {@link Windowing}
 * which groups elements from streams into {@link Window}s. The join operation
 * is performed within same windows produced on left and right side of
 * input {@link Dataset}s.
 *
 * <h3>Builders:</h3>
 * <ol>
 *   <li>{@code [named] ..................} give name to the operator [optional]
 *   <li>{@code of .......................} left and right input dataset
 *   <li>{@code by .......................} {@link UnaryFunction}s transforming left and right elements into keys
 *   <li>{@code using ....................} {@link BinaryFunctor} receiving left and right element from joined window
 *   <li>{@code [windowBy] ...............} windowing function (see {@link Windowing}), default attached windowing
 *   <li>{@code (output | outputValues) ..} build output dataset
 * </ol>
 *
 */
@Audience(Audience.Type.CLIENT)
@Recommended(
    reason =
        "Might be useful to override because of performance reasons in a "
            + "specific join types (e.g. sort join), which might reduce the space "
            + "complexity",
    state = StateComplexity.LINEAR,
    repartitions = 1
)
public class Join<LEFT, RIGHT, KEY, OUT, W extends Window>
    extends StateAwareWindowWiseOperator<Object, Either<LEFT, RIGHT>,
    Either<LEFT, RIGHT>, KEY, Pair<KEY, OUT>, W, Join<LEFT, RIGHT, KEY, OUT, W>> {

  public enum Type {
    INNER,
    LEFT,
    RIGHT,
    FULL
  }

  public static <LEFT, RIGHT> ByBuilder<LEFT, RIGHT> of(
      Dataset<LEFT> left, Dataset<RIGHT> right) {
    return new OfBuilder("Join").of(left, right);
  }

  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  public static class OfBuilder {

    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    public <LEFT, RIGHT> ByBuilder<LEFT, RIGHT> of(Dataset<LEFT> left, Dataset<RIGHT> right) {
      if (right.getFlow() != left.getFlow()) {
        throw new IllegalArgumentException("Pass inputs from the same flow");
      }
      return new ByBuilder<>(name, left, right);
    }
  }

  public static class ByBuilder<LEFT, RIGHT> {

    private final String name;
    private final Dataset<LEFT> left;
    private final Dataset<RIGHT> right;

    ByBuilder(String name, Dataset<LEFT> left, Dataset<RIGHT> right) {
      this.name = Objects.requireNonNull(name);
      this.left = Objects.requireNonNull(left);
      this.right = Objects.requireNonNull(right);
    }

    public <KEY> UsingBuilder<LEFT, RIGHT, KEY> by(
        UnaryFunction<LEFT, KEY> leftKeyExtractor,
        UnaryFunction<RIGHT, KEY> rightKeyExtractor) {
      return by(leftKeyExtractor, rightKeyExtractor, null);
    }

    public <KEY> UsingBuilder<LEFT, RIGHT, KEY> by(
        UnaryFunction<LEFT, KEY> leftKeyExtractor,
        UnaryFunction<RIGHT, KEY> rightKeyExtractor,
        @Nullable Class<KEY> keyClass) {
      return new UsingBuilder<>(
          name, left, right, leftKeyExtractor, rightKeyExtractor, keyClass);
    }
  }

  public static class UsingBuilder<LEFT, RIGHT, KEY> {

    private final String name;
    private final Dataset<LEFT> left;
    private final Dataset<RIGHT> right;
    private final UnaryFunction<LEFT, KEY> leftKeyExtractor;
    private final UnaryFunction<RIGHT, KEY> rightKeyExtractor;
    @Nullable
    private final Class<KEY> keyClass;

    UsingBuilder(String name,
                 Dataset<LEFT> left,
                 Dataset<RIGHT> right,
                 UnaryFunction<LEFT, KEY> leftKeyExtractor,
                 UnaryFunction<RIGHT, KEY> rightKeyExtractor,
                 @Nullable Class<KEY> keyClass) {
      this.name = name;
      this.left = left;
      this.right = right;
      this.leftKeyExtractor = leftKeyExtractor;
      this.rightKeyExtractor = rightKeyExtractor;
      this.keyClass = keyClass;
    }

    public <OUT> Join.WindowingBuilder<LEFT, RIGHT, KEY, OUT> using(
        BinaryFunctor<LEFT, RIGHT, OUT> functor) {
      return new Join.WindowingBuilder<>(
          name,
          left,
          right,
          leftKeyExtractor,
          rightKeyExtractor,
          keyClass,
          functor,
          Join.Type.INNER);
    }
  }

  public static class WindowingBuilder<LEFT, RIGHT, KEY, OUT>
      implements Builders.Output<Pair<KEY, OUT>>,
      Builders.OutputValues<KEY, OUT>,
      OptionalMethodBuilder<WindowingBuilder<LEFT, RIGHT, KEY, OUT>> {

    private final String name;
    private final Dataset<LEFT> left;
    private final Dataset<RIGHT> right;
    private final UnaryFunction<LEFT, KEY> leftKeyExtractor;
    private final UnaryFunction<RIGHT, KEY> rightKeyExtractor;
    @Nullable
    private final Class<KEY> keyClass;
    private final BinaryFunctor<LEFT, RIGHT, OUT> joinFunc;
    private final Type type;

    WindowingBuilder(String name,
                     Dataset<LEFT> left,
                     Dataset<RIGHT> right,
                     UnaryFunction<LEFT, KEY> leftKeyExtractor,
                     UnaryFunction<RIGHT, KEY> rightKeyExtractor,
                     @Nullable Class<KEY> keyClass,
                     BinaryFunctor<LEFT, RIGHT, OUT> joinFunc,
                     Type type) {

      this.name = Objects.requireNonNull(name);
      this.left = Objects.requireNonNull(left);
      this.right = Objects.requireNonNull(right);
      this.leftKeyExtractor = Objects.requireNonNull(leftKeyExtractor);
      this.rightKeyExtractor = Objects.requireNonNull(rightKeyExtractor);
      this.keyClass = keyClass;
      this.joinFunc = Objects.requireNonNull(joinFunc);
      this.type = Objects.requireNonNull(type);
    }

    @Override
    public Dataset<Pair<KEY, OUT>> output(OutputHint... outputHints) {
      return windowBy(null).output(outputHints);
    }

    public <W extends Window> OutputBuilder<LEFT, RIGHT, KEY, OUT, W> windowBy(
        Windowing<Either<LEFT, RIGHT>, W> windowing) {
      return new OutputBuilder<>(
          name,
          left,
          right,
          leftKeyExtractor,
          rightKeyExtractor,
          keyClass,
          joinFunc,
          type,
          windowing);
    }
  }

  public static class OutputBuilder<LEFT, RIGHT, KEY, OUT, W extends Window>
      implements Builders.OutputValues<KEY, OUT>, Builders.Output<Pair<KEY, OUT>> {

    private final String name;
    private final Dataset<LEFT> left;
    private final Dataset<RIGHT> right;
    private final UnaryFunction<LEFT, KEY> leftKeyExtractor;
    private final UnaryFunction<RIGHT, KEY> rightKeyExtractor;
    @Nullable
    private final Class<KEY> keyClass;
    private final BinaryFunctor<LEFT, RIGHT, OUT> joinFunc;
    private final Type type;

    @Nullable
    private final Windowing<Either<LEFT, RIGHT>, W> windowing;

    OutputBuilder(String name,
                  Dataset<LEFT> left,
                  Dataset<RIGHT> right,
                  UnaryFunction<LEFT, KEY> leftKeyExtractor,
                  UnaryFunction<RIGHT, KEY> rightKeyExtractor,
                  @Nullable Class<KEY> keyClass,
                  BinaryFunctor<LEFT, RIGHT, OUT> joinFunc,
                  Type type,
                  @Nullable Windowing<Either<LEFT, RIGHT>, W> windowing) {
      this.name = Objects.requireNonNull(name);
      this.left = Objects.requireNonNull(left);
      this.right = Objects.requireNonNull(right);
      this.leftKeyExtractor = Objects.requireNonNull(leftKeyExtractor);
      this.rightKeyExtractor = Objects.requireNonNull(rightKeyExtractor);
      this.keyClass = keyClass;
      this.joinFunc = Objects.requireNonNull(joinFunc);
      this.type = Objects.requireNonNull(type);
      this.windowing = windowing;
    }

    @Override
    public Dataset<Pair<KEY, OUT>> output(OutputHint... outputHints) {
      final Flow flow = left.getFlow();
      final Join<LEFT, RIGHT, KEY, OUT, W> join =
          new Join<>(
              name,
              flow,
              left,
              right,
              leftKeyExtractor,
              rightKeyExtractor,
              keyClass,
              joinFunc,
              type,
              windowing,
              Sets.newHashSet(outputHints));
      flow.add(join);
      return join.output();
    }
  }

  private final Dataset<LEFT> left;
  private final Dataset<RIGHT> right;
  private final Dataset<Pair<KEY, OUT>> output;

  @VisibleForTesting final UnaryFunction<LEFT, KEY> leftKeyExtractor;
  @VisibleForTesting final UnaryFunction<RIGHT, KEY> rightKeyExtractor;
  @Nullable
  private final Class<KEY> keyClass;

  private final BinaryFunctor<LEFT, RIGHT, OUT> functor;
  private final Type type;

  Join(String name,
       Flow flow,
       Dataset<LEFT> left, Dataset<RIGHT> right,
       UnaryFunction<LEFT, KEY> leftKeyExtractor,
       UnaryFunction<RIGHT, KEY> rightKeyExtractor,
       @Nullable Class<KEY> keyClass,
       BinaryFunctor<LEFT, RIGHT, OUT> functor,
       Type type,
       @Nullable Windowing<Either<LEFT, RIGHT>, W> windowing,
       Set<OutputHint> outputHints) {
    super(name, flow, windowing, (Either<LEFT, RIGHT> elem) -> {
      if (elem.isLeft()) {
        return leftKeyExtractor.apply(elem.left());
      }
      return rightKeyExtractor.apply(elem.right());
    });
    this.left = left;
    this.right = right;
    this.leftKeyExtractor = leftKeyExtractor;
    this.rightKeyExtractor = rightKeyExtractor;
    this.keyClass = keyClass;
    this.functor = functor;
    @SuppressWarnings("unchecked")
    Dataset<Pair<KEY, OUT>> output = createOutput((Dataset) left, outputHints);
    this.output = output;
    this.type = type;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<Dataset<Object>> listInputs() {
    return Arrays.asList((Dataset) left, (Dataset) right);
  }

  @Override
  public Dataset<Pair<KEY, OUT>> output() {
    return output;
  }

  @SuppressWarnings("unchecked")
  private static final ListStorageDescriptor LEFT_STATE_DESCR =
      ListStorageDescriptor.of("left", (Class) Object.class);
  @SuppressWarnings("unchecked")
  private static final ListStorageDescriptor RIGHT_STATE_DESCR =
      ListStorageDescriptor.of("right", (Class) Object.class);


  private abstract class AbstractJoinState implements State<Either<LEFT, RIGHT>, OUT> {

    final ListStorage<LEFT> leftElements;
    final ListStorage<RIGHT> rightElements;

    @SuppressWarnings("unchecked")
    AbstractJoinState(StorageProvider storageProvider) {
      leftElements = storageProvider.getListStorage(LEFT_STATE_DESCR);
      rightElements = storageProvider.getListStorage(RIGHT_STATE_DESCR);
    }

    @Override
    public void close() {
      leftElements.clear();
      rightElements.clear();
    }

    /**
     * This method can be triggered by all joins except INNER
     */
    void flushUnjoinedElems(Collector<OUT> context, Iterable<LEFT> lefts, Iterable<RIGHT> rights) {
      boolean leftEmpty = !lefts.iterator().hasNext();
      boolean rightEmpty = !rights.iterator().hasNext();
      // if just a one collection is empty
      if (leftEmpty != rightEmpty) {
        switch (getType()) {
          case LEFT:
            if (rightEmpty) {
              for (LEFT elem : lefts) {
                functor.apply(elem, null, context);
              }
            }
            break;
          case RIGHT:
            if (leftEmpty) {
              for (RIGHT elem : rights) {
                functor.apply(null, elem, context);
              }
            }
            break;
          case FULL:
            if (leftEmpty) {
              for (RIGHT elem : rights) {
                functor.apply(null, elem, context);
              }
            } else {
              for (LEFT elem : lefts) {
                functor.apply(elem, null, context);
              }
            }
            break;
          default:
            throw new IllegalArgumentException("Unsupported type: " + getType());

        }
      }
    }
  }

  /**
   * An implementation of the join state which will accumulate elements
   * until it is flushed at which point it then emits all elements.<p>
   *
   * (This implementation is known to work correctly with merging
   * windowing, early triggering, as well as with timed multi-window
   * windowing (e.g. time sliding.))
   */
  private class StableJoinState extends AbstractJoinState
      implements StateSupport.MergeFrom<StableJoinState> {

    StableJoinState(StorageProvider storageProvider) {
      super(storageProvider);
    }

    @Override
    public void add(Either<LEFT, RIGHT> elem) {
      if (elem.isLeft()) {
        leftElements.add(elem.left());
      } else {
        rightElements.add(elem.right());
      }
    }

    @Override
    public void flush(Collector<OUT> context) {
      Iterable<LEFT> lefts = leftElements.get();
      Iterable<RIGHT> rights = rightElements.get();
      for (LEFT l : lefts) {
        for (RIGHT r : rights) {
          functor.apply(l, r, context);
        }
      }
      if (type != Type.INNER) {
        flushUnjoinedElems(context, lefts, rights);
      }
    }

    @Override
    public void mergeFrom(StableJoinState other) {
      this.leftElements.addAll(other.leftElements.get());
      this.rightElements.addAll(other.rightElements.get());
    }
  }

  /**
   * An implementation of the join state which produces results, i.e. emits
   * output, as soon as possible. It has at least the following short comings
   * and should be used with care (see https://github.com/seznam/euphoria/issues/118
   * for more information):
   *
   * <ul>
   *   <li>This implementation will break the join operator if used with a
   *        merging windowing strategy, since items will be emitted under the
   *        hood of a non-final window.</li>
   *   <li>This implementation cannot be used together with early triggering
   *        on any windowing strategy as it will emit each identified pair
   *        only once during the whole course of the state's life cycle.</li>
   *   <li>This implementation will also break time-sliding windowing, as
   *        it will raise the watermark too quickly in downstream operators,
   *        thus, marking earlier - but actually still not too late time-sliding
   *        windows as late comers.</li>
   * </ul>
   */
  @Experimental
  private class EarlyEmittingJoinState
      extends AbstractJoinState
      implements State<Either<LEFT, RIGHT>, OUT>,
      StateSupport.MergeFrom<EarlyEmittingJoinState> {
    private final Collector<OUT> context;

    @SuppressWarnings("unchecked")
    public EarlyEmittingJoinState(StorageProvider storageProvider, Collector<OUT> context) {
      super(storageProvider);
      this.context = Objects.requireNonNull(context);
    }

    @Override
    public void add(Either<LEFT, RIGHT> elem) {
      if (elem.isLeft()) {
        leftElements.add(elem.left());
        emitJoinedElements(elem, rightElements);
      } else {
        rightElements.add(elem.right());
        emitJoinedElements(elem, leftElements);
      }
    }

    @SuppressWarnings("unchecked")
    private void emitJoinedElements(Either<LEFT, RIGHT> elem, ListStorage others) {
      assert context != null;
      if (elem.isLeft()) {
        for (Object right : others.get()) {
          functor.apply(elem.left(), (RIGHT) right, context);
        }
      } else {
        for (Object left : others.get()) {
          functor.apply((LEFT) left, elem.right(), context);
        }
      }
    }

    @Override
    public void flush(Collector<OUT> context) {
      // ~ no-op; we do all the work already on the fly
      // and flush any "pending" state _only_ when closing
      // this state
    }

    @Override
    public void close() {
      if (type != Type.INNER) {
        flushUnjoinedElems(context, leftElements.get(), rightElements.get());
      }
      super.close();
    }

    @Override
    public void mergeFrom(EarlyEmittingJoinState other) {
      Iterable<LEFT> otherLefts = other.leftElements.get();
      Iterable<RIGHT> thisRights = this.rightElements.get();
      for (LEFT l : otherLefts) {
        for (RIGHT r : thisRights) {
          functor.apply(l, r, context);
        }
      }
      Iterable<RIGHT> otherRights = other.rightElements.get();
      Iterable<LEFT> thisLefts = this.leftElements.get();
      for (RIGHT r : otherRights) {
        for (LEFT l : thisLefts) {
          functor.apply(l, r, context);
        }
      }
      this.leftElements.addAll(otherLefts);
      this.rightElements.addAll(otherRights);
    }
  }

  public Type getType() {
    return type;
  }

  public UnaryFunction<LEFT, KEY> getLeftKeyExtractor() {
    return leftKeyExtractor;
  }

  public UnaryFunction<RIGHT, KEY> getRightKeyExtractor() {
    return rightKeyExtractor;
  }

  @Nullable
  public Class<KEY> getKeyClass() {
    return keyClass;
  }

  public BinaryFunctor<LEFT, RIGHT, OUT> getJoiner() {
    return functor;
  }

  @Override
  @SuppressWarnings("unchecked")
  public DAG<Operator<?, ?>> getBasicOps() {
    final Flow flow = getFlow();

    final MapElements<LEFT, Either<LEFT, RIGHT>> leftMap = new MapElements<>(
        getName() + "::Map-left", flow, left, Either::left);

    final MapElements<RIGHT, Either<LEFT, RIGHT>> rightMap = new MapElements<>(
        getName() + "::Map-right", flow, right, Either::right);

    final Union<Either<LEFT, RIGHT>> union =
        new Union<>(getName() + "::Union", flow,
            Arrays.asList(leftMap.output(), rightMap.output()));

    final ReduceStateByKey<Either<LEFT, RIGHT>, KEY, Either<LEFT, RIGHT>, OUT, StableJoinState, W>
        reduce = new ReduceStateByKey(
        getName() + "::ReduceStateByKey",
        flow,
        union.output(),
        keyExtractor,
        e -> e,
        getWindowing(),
        (StateContext context, Collector ctx) -> {
          StorageProvider storages = context.getStorageProvider();
          return ctx == null
              ? new StableJoinState(storages)
              : new EarlyEmittingJoinState(storages, ctx);
        }, new StateSupport.MergeFromStateMerger<>(),
        getHints());

    final DAG<Operator<?, ?>> dag = DAG.of(leftMap, rightMap);
    dag.add(union, leftMap, rightMap);
    dag.add(reduce, union);
    return dag;
  }
}
