package concoord.flow;

import concoord.concurrent.Awaitable;
import org.jetbrains.annotations.NotNull;

public class Return<T> extends Break<T> {

  private final Result<T> result;

  public Return(T output) {
    this.result = new ResultMessage<T>(output);
  }

  public Return(@NotNull Awaitable<T> awaitable) {
    this.result = new ResultAwaitable<T>(awaitable);
  }

  public void apply(@NotNull FlowControl<? super T> flowControl) {
    result.apply(flowControl);
    super.apply(flowControl);
  }
}
