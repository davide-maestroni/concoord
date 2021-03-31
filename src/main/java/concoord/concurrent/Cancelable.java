package concoord.concurrent;

public interface Cancelable {

  // TODO: 31/03/21 Cancellable?

  boolean isError();

  boolean isDone();

  void cancel();
}
