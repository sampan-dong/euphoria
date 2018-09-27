package cz.seznam.euphoria.beam.io;

import static cz.seznam.euphoria.shadow.com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import cz.seznam.euphoria.core.client.io.BoundedDataSource;
import cz.seznam.euphoria.core.client.io.BoundedReader;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.Writer;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class EuphoriaIO {

  public static <T> Read<T> read(DataSource<T> dataSource, Coder<T> outputCoder){
    return new Read<>(dataSource, outputCoder);
  }


  public static <T> Write<T> write(DataSink<T> sink, int numPartitions) {
    return new Write<>(sink, numPartitions);
  }



  public static class Read<T> extends PTransform<PBegin, PCollection<T>> {

    private final DataSource<T> source;
    private final Coder<T> outputCoder;

    Read(DataSource<T> source, Coder<T> outputCoder) {
      this.source = source;
      this.outputCoder = outputCoder;
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      if (source.isBounded()) {
        org.apache.beam.sdk.io.Read.Bounded<T> bounded =
            org.apache.beam.sdk.io.Read.from(new BoundedSourceWrapper<>(source.asBounded(), this));

        return input.apply(bounded);
      } else {
        throw new UnsupportedOperationException("Unbounded is not supported for now.");
      }
    }

    public Coder<T> getOutputCoder() {
      return outputCoder;
    }
  }

  public static class BoundedSourceWrapper<T> extends BoundedSource<T>{

    private final BoundedDataSource<T> source;
    private final Read<T> readInstance;

    public BoundedSourceWrapper(BoundedDataSource<T> source, Read<T> readInstance) {
      this.source = source;
      this.readInstance = readInstance;
    }

    @Override
    public List<? extends BoundedSource<T>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {

      List<BoundedDataSource<T>> splits = source.split(desiredBundleSizeBytes);
      return splits.stream()
          .map(source -> new BoundedSourceWrapper<>(source, readInstance))
          .collect(Collectors.toList());
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      return source.sizeEstimate();
    }

    @Override
    public BoundedReader<T> createReader(PipelineOptions options) throws IOException {
      cz.seznam.euphoria.core.client.io.BoundedReader<T> boundedReader = source.openReader();
      return new BoundedReaderWrapper<>(boundedReader, this);
    }

    @Override
    public Coder<T> getOutputCoder() {
      return readInstance.getOutputCoder();
    }
  }

  public static class BoundedReaderWrapper<T> extends BoundedSource.BoundedReader<T> {
    private final BoundedReader<T> euphoriaReader;
    private final BoundedSourceWrapper<T> boundedSourceWrapper;
    private T currentElement;

    public BoundedReaderWrapper(
        BoundedReader<T> euphoriaReader, BoundedSourceWrapper<T> boundedSourceWrapper) {
      this.euphoriaReader = euphoriaReader;
      this.boundedSourceWrapper = boundedSourceWrapper;
    }

    @Override
    public boolean start() throws IOException {
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      boolean hasNext = euphoriaReader.hasNext();
      if(hasNext){
        currentElement = euphoriaReader.next();
      }
      return hasNext;
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      return currentElement;
    }

    @Override
    public void close() throws IOException {
      euphoriaReader.close();
    }

    @Override
    public BoundedSource<T> getCurrentSource() {
      return boundedSourceWrapper;
    }

  }

  /**
   * Write transform for euphoria sinks.
   *
   * @param <T> type of data to write
   */
  public static class Write<T> extends PTransform<PCollection<T>, PDone> {

    private final DataSink<T> sink;
    private final int numPartitions;

    private Write(DataSink<T> sink, int numPartitions) {
      this.sink = sink;
      this.numPartitions = numPartitions;
    }
    @Override
    public PDone expand(PCollection<T> input) {
      checkArgument(
          input.isBounded() == PCollection.IsBounded.BOUNDED,
          "Write supports bounded PCollections only.");

      // right now this is probably the best place to initialize sink
      sink.initialize();

      input
          .apply("write-result", ParDo.of(new WriteFn<>(sink)))
          .apply(Combine.globally((it) ->
              (int) StreamSupport.stream(it.spliterator(), false).count()))
          .apply(ParDo.of(new DoFn<Integer, Void>() {

            @SuppressWarnings("unused")
            @ProcessElement
            public void processElement() throws IOException {
              sink.commit();
            }
          }));

      return PDone.in(input.getPipeline());
    }
  }

  private static class PartitionFn<T> extends DoFn<T, KV<Integer, T>> {

    private final int numPartitions;

    PartitionFn(int numPartitions) {
      this.numPartitions = numPartitions;
    }

    @SuppressWarnings("unused")
    @ProcessElement
    public void processElement(
        @Element T element, OutputReceiver<KV<Integer, T>> outputReceiver) {
      final int partition = (element.hashCode() & Integer.MAX_VALUE) % numPartitions;
      outputReceiver.output(KV.of(partition, element));
    }
  }

  private static class WriteFn<T> extends DoFn<T, Integer> {

    Random random = new Random();
    final Writer<T> writer;

    WriteFn(DataSink<T> sink) {
      writer  = sink.openWriter(random.nextInt());
    }


    @SuppressWarnings("unused")
    @ProcessElement
    public void processElement(@Element T element) throws IOException {
        writer.write(element);
    }

    @SuppressWarnings("unused")
    @FinishBundle
    public void finishBatch() throws IOException {
      try {

        writer.flush();
        writer.commit();

      } catch (IOException e) {
        writer.rollback();
      } finally {
        writer.close();
      }
    }
  }
}
