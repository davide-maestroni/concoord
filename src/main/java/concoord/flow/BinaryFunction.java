package concoord.flow;

import org.jetbrains.annotations.NotNull;

public interface BinaryFunction<T, P1, P2> {

  @NotNull
  Result<T> call(P1 firstParam, P2 secondParam) throws Exception;
}
