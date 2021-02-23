package concoord.lang;

import concoord.concurrent.Awaiter;
import org.jetbrains.annotations.NotNull;

class DummyAwaiter<T> implements Awaiter<T> {

  public void message(T message) {
  }

  public void error(@NotNull Throwable error) {
  }

  public void end() {
  }
}
