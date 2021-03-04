package concoord.concurrent;

public interface Cancelable {

  boolean isError();

  boolean isDone();

  void cancel();
}
