package concoord.flow;

import org.jetbrains.annotations.NotNull;

public interface NullaryInvocation<T> {

  @NotNull
  Result<T> call() throws Exception;
}
