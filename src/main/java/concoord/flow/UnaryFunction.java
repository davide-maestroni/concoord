package concoord.flow;

import org.jetbrains.annotations.NotNull;

public interface UnaryFunction<T, P1> {

  @NotNull
  Result<T> call(P1 firstParam) throws Exception;
}
