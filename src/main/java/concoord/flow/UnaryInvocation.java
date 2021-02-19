package concoord.flow;

import org.jetbrains.annotations.NotNull;

public interface UnaryInvocation<T, P1> {

  @NotNull
  Result<T> call(P1 firstParam) throws Exception;
}
