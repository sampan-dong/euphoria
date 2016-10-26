package cz.seznam.euphoria.flink.functions;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.Context;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import java.util.Objects;

public class UnaryFunctorWrapper<WID extends Window, IN, OUT>
    implements FlatMapFunction<WindowedElement<WID, IN>,
                               WindowedElement<WID, OUT>>,
               ResultTypeQueryable<WindowedElement<WID, OUT>>
{
  private final UnaryFunctor<IN, OUT> f;

  public UnaryFunctorWrapper(UnaryFunctor<IN, OUT> f) {
    this.f = Objects.requireNonNull(f);
  }

  @Override
  public void flatMap(WindowedElement<WID, IN> value,
                      Collector<WindowedElement<WID, OUT>> out)
      throws Exception
  {
    f.apply(value.get(), new Context<OUT>() {
      @Override
      public void collect(OUT elem) {
        out.collect(new WindowedElement<>(value.getWindow(), elem));
      }
      @Override
      public Object getWindow() {
        return value.getWindow();
      }
    });
  }

  @Override
  public TypeInformation<WindowedElement<WID, OUT>> getProducedType() {
    return TypeInformation.of((Class) WindowedElement.class);
  }
}