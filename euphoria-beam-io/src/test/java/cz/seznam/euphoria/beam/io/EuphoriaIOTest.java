package cz.seznam.euphoria.beam.io;

import cz.seznam.euphoria.core.client.io.ListDataSource;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static java.util.Arrays.asList;

public class EuphoriaIOTest {

  @Rule
  public TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void boundedReadTest() {

    List<String> inputList = asList(
        "one", "two", "three", "four", "five",
        "one two three four four two two",
        "one one one two two three");
    ListDataSource<String> input = ListDataSource.bounded(inputList);

    PCollection<String> output = testPipeline.apply(EuphoriaIO.read(input, StringUtf8Coder.of()));

    PAssert.that(output).containsInAnyOrder(inputList);

    testPipeline.run().waitUntilFinish();

  }
}
