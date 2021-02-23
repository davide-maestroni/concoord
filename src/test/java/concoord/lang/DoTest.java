package concoord.lang;

import static org.assertj.core.api.Assertions.assertThat;

import concoord.concurrent.Awaitable;
import concoord.concurrent.Awaiter;
import concoord.concurrent.LazyExecutor;
import concoord.concurrent.ScheduledExecutor;
import concoord.flow.Return;
import concoord.flow.Yield;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

public class DoTest {

  @Test
  public void basic() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor);
    Awaitable<String> awaitable = new Do<>(() -> new Return<>("hello")).on(scheduler);
    AtomicReference<String> testMessage = new AtomicReference<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicBoolean testEnd = new AtomicBoolean();
    awaitable.await(Integer.MAX_VALUE, new Awaiter<String>() {
      @Override
      public void message(String message) {
        testMessage.set(message);
      }

      @Override
      public void error(Throwable error) {
        testError.set(error);
      }

      @Override
      public void end() {
        testEnd.set(true);
      }
    });
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessage.get()).isEqualTo("hello");
    assertThat(testError.get()).isNull();
    assertThat(testEnd.get()).isTrue();
  }

  @Test
  public void noEnd() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor);
    Awaitable<String> awaitable = new Do<>(() -> new Return<>("hello")).on(scheduler);
    AtomicReference<String> testMessage = new AtomicReference<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicBoolean testEnd = new AtomicBoolean();
    awaitable.await(1, new Awaiter<String>() {
      @Override
      public void message(String message) {
        testMessage.set(message);
      }

      @Override
      public void error(Throwable error) {
        testError.set(error);
      }

      @Override
      public void end() {
        testEnd.set(true);
      }
    });
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessage.get()).isEqualTo("hello");
    assertThat(testError.get()).isNull();
    assertThat(testEnd.get()).isFalse();
  }

  @Test
  public void multiAwaitLazy() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor);
    Awaitable<String> awaitable = new Do<>(() -> new Return<>("hello")).on(scheduler);
    AtomicReference<String> testMessage = new AtomicReference<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicBoolean testEnd = new AtomicBoolean();
    Awaiter<String> awaiter = new Awaiter<String>() {
      @Override
      public void message(String message) {
        testMessage.set(message);
      }

      @Override
      public void error(Throwable error) {
        testError.set(error);
      }

      @Override
      public void end() {
        testEnd.set(true);
      }
    };
    awaitable.await(1, awaiter);
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessage.get()).isEqualTo("hello");
    assertThat(testError.get()).isNull();
    assertThat(testEnd.get()).isFalse();

    testMessage.set(null);
    awaitable.await(1, awaiter);
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessage.get()).isNull();
    assertThat(testError.get()).isNull();
    assertThat(testEnd.get()).isTrue();
  }

  @Test
  public void multiAwaitActive() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor);
    Awaitable<String> awaitable = new Do<>(() -> new Return<>("hello")).on(scheduler);
    AtomicReference<String> testMessage = new AtomicReference<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicBoolean testEnd = new AtomicBoolean();
    Awaiter<String> awaiter = new Awaiter<String>() {
      @Override
      public void message(String message) {
        testMessage.set(message);
      }

      @Override
      public void error(Throwable error) {
        testError.set(error);
      }

      @Override
      public void end() {
        testEnd.set(true);
      }
    };
    awaitable.await(1, awaiter);
    awaitable.await(1, awaiter);
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessage.get()).isEqualTo("hello");
    assertThat(testError.get()).isNull();
    assertThat(testEnd.get()).isTrue();
  }

  @Test
  public void endless() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor);
    Awaitable<String> awaitable = new Do<>(() -> new Yield<>("hello")).on(scheduler);
    ArrayList<String> testMessages = new ArrayList<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicBoolean testEnd = new AtomicBoolean();
    awaitable.await(11, new Awaiter<String>() {
      @Override
      public void message(String message) {
        testMessages.add(message);
      }

      @Override
      public void error(Throwable error) {
        testError.set(error);
      }

      @Override
      public void end() {
        testEnd.set(true);
      }
    });
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessages).hasSize(11).containsOnly("hello");
    assertThat(testError.get()).isNull();
    assertThat(testEnd.get()).isFalse();
  }
}
