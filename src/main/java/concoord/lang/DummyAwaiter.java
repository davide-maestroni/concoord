package concoord.lang;

import concoord.concurrent.Awaiter;

class DummyAwaiter<T> implements Awaiter<T> {

  public void message(T message) {
  }

  public void error(Throwable error) {
  }

  public void end() {
  }
}
