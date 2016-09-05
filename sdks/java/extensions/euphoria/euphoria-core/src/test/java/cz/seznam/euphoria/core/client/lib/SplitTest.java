package cz.seznam.euphoria.core.client.lib;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryPredicate;
import cz.seznam.euphoria.core.client.operator.Filter;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.Util;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.*;

public class SplitTest {

  @Test
  public void testBuild() {
    String opName = "split";
    Flow flow = Flow.create("split-test");
    Dataset<String> dataset = Util.createMockDataset(flow, 1);

    Split.Output<String> split = Split.named(opName)
        .of(dataset)
        .using((UnaryPredicate<String>) what -> true)
        .output();

    assertEquals(2, flow.size());
    Filter positive =
        (Filter) getOperator(flow, opName + Split.POSITIVE_FILTER_SUFFIX);
    assertSame(flow, positive.getFlow());
    assertNotNull(positive.getPredicate());
    assertSame(positive.output(), split.positive());
    Filter negative =
        (Filter) getOperator(flow, opName + Split.NEGATIVE_FILTER_SUFFIX);
    assertSame(flow, negative.getFlow());
    assertNotNull(negative.getPredicate());
    assertSame(negative.output(), split.negative());
  }

  @Test
  public void testBuild_ImplicitName() {
    Flow flow = Flow.create("split-test");
    Dataset<String> dataset = Util.createMockDataset(flow, 1);

    Split.of(dataset)
        .using((UnaryPredicate<String>) what -> true)
        .output();

    assertNotNull(
        getOperator(flow, Split.DEFAULT_NAME + Split.POSITIVE_FILTER_SUFFIX));
    assertNotNull(
        getOperator(flow, Split.DEFAULT_NAME + Split.NEGATIVE_FILTER_SUFFIX));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testBuild_NegatedPredicate() {
    Flow flow = Flow.create("split-test");
    Dataset<Integer> dataset = Util.createMockDataset(flow, 1);

    Split.of(dataset)
        .using((UnaryPredicate<Integer>) what -> what % 2 == 0)
        .output();

    Filter<Integer> oddNumbers = (Filter<Integer>) getOperator(
        flow, Split.DEFAULT_NAME + Split.NEGATIVE_FILTER_SUFFIX);
    assertFalse(oddNumbers.getPredicate().apply(0));
    assertFalse(oddNumbers.getPredicate().apply(2));
    assertFalse(oddNumbers.getPredicate().apply(4));
    assertTrue(oddNumbers.getPredicate().apply(1));
    assertTrue(oddNumbers.getPredicate().apply(3));
    assertTrue(oddNumbers.getPredicate().apply(5));
  }

  private Operator<?, ?> getOperator(Flow flow, String name) {
    Optional<Operator<?, ?>> op = flow.operators().stream()
        .filter(o -> o.getName().equals(name))
        .findFirst();
    return op.isPresent() ? op.get() : null;
  }

}