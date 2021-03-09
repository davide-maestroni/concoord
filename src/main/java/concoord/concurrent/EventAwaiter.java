package concoord.concurrent;

public interface EventAwaiter<T> {

  void event(T event) throws Exception;
}
