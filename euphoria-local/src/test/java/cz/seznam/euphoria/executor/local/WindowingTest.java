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
package cz.seznam.euphoria.executor.local;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Count;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.AssignEventTime;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.ReduceWindow;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.shadow.com.google.common.collect.Sets;
import cz.seznam.euphoria.core.client.util.Fold;
import cz.seznam.euphoria.testing.DatasetAssert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WindowingTest {

  private LocalExecutor executor;

  @Before
  public void setUp() {
    executor = new LocalExecutor();
  }


  static final class Item {
    final String word;
    final long evtTs;

    Item(String word, long evtTs) {
      this.word = word;
      this.evtTs = evtTs;
    }

    @Override
    public String toString() {
      return "Item{" +
          "word='" + word + '\'' +
          ", evtTs=" + evtTs +
          '}';
    }
  }

  @Test
  public void testWindowingByEventTime() throws Exception {
    // we have to make sure the timestamp is in the future
    ListDataSource<Item> input = ListDataSource.bounded(asList(
        new Item("one", 3001),
        new Item("one", 3500),
        new Item("one", 1000),
        new Item("one", 2100),
        new Item("one", 2050),
        new Item("one", 2099),
        new Item("two", 1100),
        new Item("two", 1500),
        new Item("two", 1900),
        new Item("two", 2199),
        new Item("two", 2355),
        new Item("four", 1500),
        new Item("four", 1200),
        new Item("three", 1000),
        new Item("three", 2831),
        new Item("three", 2123)));

    Flow flow = Flow.create("Test");
    Dataset<Item> lines = flow.createInput(input);
    lines = AssignEventTime.of(lines).using(i -> i.evtTs).output();

    Dataset<Pair<Item, Long>> words = MapElements.of(lines)
        .using((UnaryFunction<Item, Pair<Item, Long>>) item -> Pair.of(item, 1L))
        .output();

    Dataset<Pair<String, Long>> reduced = ReduceByKey
        .of(words)
        .keyBy(e -> e.getFirst().word)
        .valueBy(Pair::getSecond)
        .combineBy(Sums.ofLongs())
        .windowBy(Time.of(Duration.ofSeconds(1)))
        .output();

    Dataset<String> mapped = MapElements.of(reduced)
        .using(p -> p.getFirst() + "-" + p.getSecond())
        .output();

    mapped = FlatMap.of(mapped)
        .using((UnaryFunctor<String, String>) (elem, c) -> {
          TimeInterval w = (TimeInterval) c.getWindow();
          c.collect(w.getStartMillis() / 1000L + ": " + elem);
        })
        .output();

    ListDataSink<String> output = ListDataSink.get();
    mapped.persist(output);

    executor.setTriggeringSchedulerSupplier(() -> new WatermarkTriggerScheduler(0));
    executor.submit(flow).get();

    DatasetAssert.unorderedEquals(
        output.getOutputs(),
        /* 1st second window */ "1: one-1", "1: two-3", "1: three-1", "1: four-2",
        /* 2nd second window */ "2: one-3", "2: two-2", "2: three-2",
        /* 3rd second window */ "3: one-2");
  }

  @Test
  public void testAttachedWindowing_ContinuousOutput() throws InterruptedException,
      ExecutionException {
    final Duration READ_DELAY = Duration.ofMillis(73L);
    Flow flow = Flow.create("Test");

    // ~ one partition; supplying every READ_DELAYS a new element
    Dataset<String> input = flow.createInput(ListDataSource.unbounded(
        asList(("r-one r-two r-three s-one s-two s-three t-one")
            .split(" "))
    ).withReadDelay(READ_DELAY));

    // ~ emits after 3 input elements received due to "count windowing"
    Dataset<Set<String>> first = ReduceWindow.of(input)
        .reduceBy(s -> s.collect(Collectors.toSet()))
        .windowBy(Count.of(3))
        .output();

    // ~ consume the output of the previous operator and forward to the next
    // serving merely as another operator in the pipeline; must emit its
    // inputs elements as soon they arrive (note: this is a
    // non-window-wise operator)
    Dataset<Set<String>> mediator = MapElements.of(first)
        .using(e -> e)
        .output();

    // ~ a window-wise operator with the default "attached windowing". it's
    // attaching itself to the windowing of the preceding window-wise operator
    // in the pipeline and is supposed to emit equivalent windows as that one.
    // further, the operator is supposed to emit the windows as soon as possible,
    // i.e. once the last item for a window from the preceding window-wise operator
    // is received.
    Dataset<Set<String>> second = ReduceWindow.of(mediator)
        .reduceBy(Fold.of(Sets::union))
        .output();

    // ~ consume the output and put a timestamp on each element. emits output
    // itself as soon as it receives input, due to the operator's streaming nature.
    Dataset<Pair<Long, Set<String>>> third = MapElements.of(second)
        .using(what -> Pair.of(System.currentTimeMillis(), what))
        .output();

    ListDataSink<Pair<Long, Set<String>>> output = ListDataSink.get();
    third.persist(output);

    executor.submit(flow).get();

    List<Pair<Long, Set<String>>> ordered =
        output.getOutputs()
            .stream()
            .sorted(Comparator.comparing(Pair::getFirst))
            .collect(Collectors.toList());
    assertEquals(3, ordered.size());
    // ~ test that we receive the first element earlier than the second one
    assertSmaller(
        ordered.get(0).getFirst() + 2 * READ_DELAY.toMillis(), ordered.get(1).getFirst());
    assertEquals(Sets.newHashSet("r-one", "r-two", "r-three"),
        ordered.get(0).getSecond());
    assertEquals(Sets.newHashSet("s-one", "s-two", "s-three"),
        ordered.get(1).getSecond());
    assertEquals(Sets.newHashSet("t-one"),
        ordered.get(2).getSecond());
  }

  private void assertSmaller(long x, long y) {
    assertTrue("Expected x < y but it's not with x=" + x + " and y=" + y, x < y);
  }


  @Test
  public void testAttachedWindowing_InhertitedLabel() throws Exception {
    Flow flow = Flow.create("Test");

    Dataset<Pair<String, Long>> input =
        flow.createInput(ListDataSource.unbounded(asList(
            Pair.of("one", 2000000000000L),
            Pair.of("one", 2000000000001L),
            Pair.of("two", 2000000000002L),
            Pair.of("two", 2000000000003L),
            Pair.of("three", 2000000000004L),
            Pair.of("four", 2000000000005L),
            Pair.of("five", 2000000000006L),
            Pair.of("one", 2000000001001L),
            Pair.of("two", 2000000001002L),
            Pair.of("three", 2000000001003L),
            Pair.of("four", 2000000001004L)))
            .withReadDelay(Duration.ofMillis(100L)));
    input = AssignEventTime.of(input).using(Pair::getSecond).output();

    Dataset<Pair<String, Void>> distinct =
        ReduceByKey.of(input)
            .keyBy(Pair::getFirst)
            .valueBy(e -> (Void) null)
            .combineBy(e -> null)
            .windowBy(Time.of(Duration.ofSeconds(1)))
            .output();

    Dataset<TimeInterval> windows = FlatMap.of(distinct)
        .using((UnaryFunctor<Pair<String, Void>, TimeInterval>)
            (elem, context) -> context.collect((TimeInterval) context.getWindow()))
        .output();

    Dataset<Pair<TimeInterval, Long>> counts = ReduceByKey.of(windows)
        .keyBy(e -> e)
        .valueBy(e -> 1L)
        .combineBy(Sums.ofLongs())
        .output();

    ListDataSink<Pair<TimeInterval, Long>> output = ListDataSink.get();
    counts.persist(output);

    executor.submit(flow).get();

    assertEquals(2, output.getOutputs().size());
    List<Pair<Long, Long>> ordered = output.getOutputs()
        .stream()
        .sorted(Comparator.comparing(e -> e.getFirst().getStartMillis()))
        .map(e -> Pair.of(e.getFirst().getStartMillis(), e.getSecond()))
        .collect(Collectors.toList());
    assertEquals(asList(
        Pair.of(2000000000000L, 5L), Pair.of(2000000001000L, 4L)),
        ordered);
  }

  @Test
  public void testWindowing_EndOfWindow_RBK() throws InterruptedException, ExecutionException {
    final Duration READ_DELAY = Duration.ofMillis(50L);
    Flow flow = Flow.create("Test");

    executor.setTriggeringSchedulerSupplier(
        () -> new WatermarkTriggerScheduler(1));


    Dataset<String> input = flow.createInput(ListDataSource.unbounded(
        asList("0-one 1-two 0-three 1-four 0-five 1-six 0-seven".split(" "))
    ).withReadDelay(READ_DELAY)
        .withFinalDelay(READ_DELAY.multipliedBy(2)));

    // ~ create windows of size three
    Dataset<Pair<String, Set<String>>> first =
        ReduceByKey.of(input)
            .keyBy(e -> "")
            .valueBy(e -> e)
            .reduceBy(s -> s.collect(Collectors.toSet()))
            .windowBy(Count.of(3))
            .output();

    // ~ strip the needless key and flatten out the elements thereby
    // creating multiple elements in the output belonging to the same window
    Dataset<String> second = FlatMap.of(first)
        .using((UnaryFunctor<Pair<String, Set<String>>, String>) (e, c) -> {
          e.getSecond().stream().forEachOrdered(c::collect);
        })
        .output();

    // ~ we now expect to reconstruct the same windowing
    // as the very initial step
    Dataset<Set<String>> third =
        ReduceWindow.of(second)
            .valueBy(e -> e)
            .reduceBy(s -> s.collect(Collectors.toSet()))
            .output();

    ListDataSink<Set<String>> out = ListDataSink.get();
    third.persist(out);

    executor.submit(flow).get();

    DatasetAssert.unorderedEquals(
        out.getOutputs(),
        Sets.newHashSet("0-one", "1-two", "0-three"),
        Sets.newHashSet("1-four", "0-five", "1-six"),
        Sets.newHashSet("0-seven"));
    }

}
