package concoord.concurrent;

import concoord.util.assertion.IfNull;
import concoord.util.assertion.IfSomeOf;
import org.jetbrains.annotations.NotNull;

public class CombinedAwaiter<T> implements Awaiter<T> {

  private final EventAwaiter<? super T> messageAwaiter;
  private final EventAwaiter<? super Throwable> errorAwaiter;
  private final EventAwaiter<? super Integer> endAwaiter;

  public CombinedAwaiter(@NotNull EventAwaiter<? super T> messageAwaiter,
      @NotNull EventAwaiter<? super Throwable> errorAwaiter,
      @NotNull EventAwaiter<? super Integer> endAwaiter) {
    new IfSomeOf(
        new IfNull("messageAwaiter", messageAwaiter),
        new IfNull("errorAwaiter", errorAwaiter),
        new IfNull("endAwaiter", endAwaiter)
    ).throwException();
    this.messageAwaiter = messageAwaiter;
    this.errorAwaiter = errorAwaiter;
    this.endAwaiter = endAwaiter;
  }

  public void message(T message) throws Exception {
    messageAwaiter.event(message);
  }

  public void error(@NotNull Throwable error) throws Exception {
    errorAwaiter.event(error);
  }

  public void end(int reason) throws Exception {
    endAwaiter.event(reason);
  }
}
