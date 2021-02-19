package concoord.flow;

import org.jetbrains.annotations.NotNull;

public class Break<T> implements Result<T> {

  public void apply(@NotNull FlowControl<? super T> flowControl) {
    flowControl.stop();
  }
}
