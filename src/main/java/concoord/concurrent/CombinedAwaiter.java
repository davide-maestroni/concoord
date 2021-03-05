package concoord.concurrent;

import concoord.util.assertion.IfNull;
import concoord.util.assertion.IfSomeOf;
import org.jetbrains.annotations.NotNull;

public class CombinedAwaiter<T> implements Awaiter<T> {

  private final UnaryAwaiter<? super T> messageAwaiter;
  private final UnaryAwaiter<? super Throwable> errorAwaiter;
  private final NullaryAwaiter endAwaiter;

  public CombinedAwaiter(@NotNull UnaryAwaiter<? super T> messageAwaiter,
      @NotNull UnaryAwaiter<? super Throwable> errorAwaiter, @NotNull NullaryAwaiter endAwaiter) {
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

  public void end() throws Exception {
    endAwaiter.event();
  }
}
