package concoord.flow;

import org.jetbrains.annotations.NotNull;

public interface NullaryFunction<T> {

  @NotNull
  Result<T> call() throws Exception;
}
