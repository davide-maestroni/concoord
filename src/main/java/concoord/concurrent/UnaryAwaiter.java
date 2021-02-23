package concoord.concurrent;

public interface UnaryAwaiter<T> {

  void event(T event) throws Exception;
}
