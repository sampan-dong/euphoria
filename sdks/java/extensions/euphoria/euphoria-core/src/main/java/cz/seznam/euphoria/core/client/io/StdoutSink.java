package cz.seznam.euphoria.core.client.io;

import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.core.util.URIParams;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;

/**
 * A sink to write to a specified print stream (typically
 * {@link java.lang.System#out}) using the produce element's
 * {@link Object#toString()} implementation.
 */
public class StdoutSink<T> implements DataSink<T> {

  static final long START_SYSTEM_NANO_TIME = System.nanoTime();

  public static class Factory implements DataSinkFactory {
    @Override
    public <T> DataSink<T> get(URI uri, Settings settings) {
      settings = settings.nested(URIParams.of(uri).getStringParam("cfg", null));

      String discrim = uri.getPath();
      if (discrim != null) {
        if (discrim.startsWith("/")) {
          discrim = discrim.substring(1);
        }
        discrim = discrim.trim();
        if (discrim.isEmpty()) {
          discrim = null;
        }
      }

      boolean debug = settings.getBoolean("debug", false);
      return new StdoutSink<>(debug, discrim);
    }
  }

  static abstract class AbstractWriter<T> implements Writer<T> {
    final PrintStream out;
    // ~ if 'true' 'out' will be closed, if false 'out' will be
    // kept open even after this writer is closed
    final boolean doClose;

    AbstractWriter(PrintStream out, boolean doClose) {
      this.out = out;
      this.doClose = doClose;
    }

    @Override
    public void commit() throws IOException {
      out.flush();
    }

    @Override
    public void close() throws IOException {
      if (doClose) {
        out.close();
      }
    }
  }

  static final class PlainWriter<T> extends AbstractWriter<T> {
    PlainWriter(PrintStream out, boolean doClose) {
      super(out, doClose);
    }

    @Override
    public void write(T elem) throws IOException {
      out.println(elem);
    }
  }

  static final class DebugWriter<T> extends AbstractWriter<T> {
    final int partitionId;
    final StringBuilder buf = new StringBuilder();
    final int bufResetPos;

    DebugWriter(PrintStream out, int partitionId, boolean doClose, String discriminator) {
      super(out, doClose);
      this.partitionId = partitionId;
      if (discriminator != null && !discriminator.isEmpty()) {
        buf.append(discriminator).append("> ");
      }
      this.bufResetPos = buf.length();
    }

    @Override
    public void write(T elem) throws IOException {
      // ~ make sure to issue only _one_ `out.println()` call to
      // avoid messing up the output with concurrent threads trying
      // to do the same
      buf.setLength(bufResetPos);
      buf.append((System.nanoTime() - START_SYSTEM_NANO_TIME) / 1_000_000_000.0)
          .append(": (")
          .append(Thread.currentThread().getName())
          .append(") [")
          .append(partitionId)
          .append("] (#")
          .append(System.identityHashCode(elem))
          .append("): ")
          .append(elem);
      out.println(buf);
    }
  }

  private final boolean debug;
  private final String discriminator;

  StdoutSink(boolean debug, String discriminator) {
    this.debug = debug;
    this.discriminator = discriminator;
  }

  @Override
  public Writer<T> openWriter(int partitionId) {
    // ~ we're specifying the writers _not_ to close
    // the given PrintStream (stdout here)
    PrintStream out = System.out;
    return debug
        ? new DebugWriter<>(out, partitionId, false, discriminator)
        : new PlainWriter<>(out, false);
  }

  @Override
  public void commit() throws IOException {
  }

  @Override
  public void rollback() {
  }
}