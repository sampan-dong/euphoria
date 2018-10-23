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
package cz.seznam.euphoria.hadoop.output;

import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.Writer;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Objects;

/**
 * A convenience data sink based on hadoop's {@link TextOutputFormat} for
 * directly accepting any values and rendering them using their
 * {@link Object#toString()} implementation as text.
 */
public class HadoopToStringSink<T> implements DataSink<T> {

  private final HadoopTextFileSink<String, NullWritable> impl;

  /**
   * Convenience constructor invoking {@link #HadoopToStringSink(String, Configuration)}
   * with a newly created hadoop configuration.
   *
   * @param path the path where to place the output to
   *
   * @throws NullPointerException if any of the given parameters is {@code null}
   */
  public HadoopToStringSink(String path) {
    this(path, new Configuration());
  }

  /**
   * Constructs a data sink based on hadoop's {@link TextOutputFormat}.
   * The specified path is automatically set/overridden in the given hadoop
   * configuration. Writer can create empty files
   * ({@link org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat} is not used).
   *
   * @param path the path to read data from
   * @param hadoopConfig the hadoop configuration to build on top of
   *
   * @throws NullPointerException if any of the parameters is {@code null}
   */
  @SuppressWarnings("unchecked")
  public HadoopToStringSink(String path, Configuration hadoopConfig) {
    this(path, hadoopConfig, false);
  }

  /**
   * Constructs a data sink based on hadoop's {@link TextOutputFormat}. The specified path is
   * automatically set/overridden in the given hadoop configuration.
   *
   * @param path the path to read data from
   * @param hadoopConfig the hadoop configuration to build on top of
   * @param useLazyOutputFormat whether to use {@link
   *     org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat} (won't create empty files)
   * @throws NullPointerException if any of the parameters is {@code null}
   */
  @SuppressWarnings("unchecked")
  public HadoopToStringSink(String path, Configuration hadoopConfig, boolean useLazyOutputFormat) {
    impl = new HadoopTextFileSink<>(path, hadoopConfig, useLazyOutputFormat);
  }

  @Override
  public void initialize() {
    impl.initialize();
  }

  @Override
  public Writer<T> openWriter(int partitionId) {
    return new WriterAdapter<>(impl.openWriter(partitionId));
  }

  @Override
  public void commit() throws IOException {
    impl.commit();
  }

  @Override
  public void rollback() throws IOException {
    impl.rollback();
  }

  private static final class WriterAdapter<E> implements Writer<E> {
    private final Writer<KV<String, NullWritable>> impl;

    WriterAdapter(Writer<KV<String, NullWritable>> impl) {
      this.impl = Objects.requireNonNull(impl);
    }

    @Override
    public void write(E elem) throws IOException {
      impl.write(KV.of(elem == null ? null : elem.toString(), NullWritable.get()));
    }

    @Override
    public void commit() throws IOException {
      impl.commit();
    }

    @Override
    public void close() throws IOException {
      impl.close();
    }
  }
}
