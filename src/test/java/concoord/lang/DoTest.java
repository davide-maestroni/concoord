package concoord.lang;

import static org.assertj.core.api.Assertions.assertThat;

import concoord.concurrent.Awaitable;
import concoord.concurrent.Awaiter;
import concoord.concurrent.Cancelable;
import concoord.concurrent.LazyExecutor;
import concoord.concurrent.ScheduledExecutor;
import concoord.flow.Return;
import concoord.flow.Yield;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

public class DoTest {

//  @BeforeAll
//  public static void setup() {
//    new Logger().addPrinter(new LoggingPrinter().configure(Level.INFO));
//  }
//
//  @BeforeEach
//  public void start(TestInfo testInfo) {
//    new Logger(this).log(new InfMessage("starting: %s", testInfo.getDisplayName()));
//  }

  @Test
  public void basic() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor);
    Awaitable<String> awaitable = new Do<>(() -> new Return<>("hello")).on(scheduler);
    AtomicReference<String> testMessage = new AtomicReference<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicInteger testEnd = new AtomicInteger(-1);
    awaitable.await(Integer.MAX_VALUE, testMessage::set, testError::set, testEnd::set);
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessage).hasValue("hello");
    assertThat(testError).hasValue(null);
    assertThat(testEnd).hasValue(Awaiter.DONE);
  }

  @Test
  public void noEnd() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor);
    Awaitable<String> awaitable = new Do<>(() -> new Return<>("hello")).on(scheduler);
    AtomicReference<String> testMessage = new AtomicReference<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicInteger testEnd = new AtomicInteger(-1);
    awaitable.await(1, testMessage::set, testError::set, testEnd::set);
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessage).hasValue("hello");
    assertThat(testError).hasValue(null);
    assertThat(testEnd).hasValue(-1);
  }

  @Test
  public void multiAwaitLazy() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor);
    Awaitable<String> awaitable = new Do<>(() -> new Return<>("hello")).on(scheduler);
    AtomicReference<String> testMessage = new AtomicReference<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicInteger testEnd = new AtomicInteger(-1);
    Awaiter<String> awaiter = new Awaiter<String>() {
      @Override
      public void message(String message) {
        testMessage.set(message);
      }

      @Override
      public void error(@NotNull Throwable error) {
        testError.set(error);
      }

      @Override
      public void end(int reason) {
        testEnd.set(reason);
      }
    };
    awaitable.await(1, awaiter);
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessage).hasValue("hello");
    assertThat(testError).hasValue(null);
    assertThat(testEnd).hasValue(-1);

    testMessage.set(null);
    awaitable.await(1, awaiter);
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessage).hasValue(null);
    assertThat(testError).hasValue(null);
    assertThat(testEnd).hasValue(0);
  }

  @Test
  public void multiAwaitActive() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor);
    Awaitable<String> awaitable = new Do<>(() -> new Return<>("hello")).on(scheduler);
    AtomicReference<String> testMessage = new AtomicReference<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicInteger testEnd = new AtomicInteger(-1);
    Awaiter<String> awaiter = new Awaiter<String>() {
      @Override
      public void message(String message) {
        testMessage.set(message);
      }

      @Override
      public void error(@NotNull Throwable error) {
        testError.set(error);
      }

      @Override
      public void end(int reason) {
        testEnd.set(reason);
      }
    };
    awaitable.await(1, awaiter);
    awaitable.await(1, awaiter);
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessage).hasValue("hello");
    assertThat(testError).hasValue(null);
    assertThat(testEnd).hasValue(0);
  }

  @Test
  public void endless() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor);
    Awaitable<String> awaitable = new Do<>(() -> new Yield<>("hello")).on(scheduler);
    ArrayList<String> testMessages = new ArrayList<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicInteger testEnd = new AtomicInteger(-1);
    awaitable.await(11, testMessages::add, testError::set, testEnd::set);
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessages).hasSize(11).containsOnly("hello");
    assertThat(testError).hasValue(null);
    assertThat(testEnd).hasValue(-1);
  }

  @Test
  public void first() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor);
    Awaitable<String> awaitable = new Do<>(
        () -> new Return<>(new Iter<>("1", "2", "3").on(scheduler))
    ).on(scheduler);
    AtomicReference<String> testMessage = new AtomicReference<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicInteger testEnd = new AtomicInteger(-1);
    awaitable.await(1, testMessage::set, testError::set, testEnd::set);
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessage).hasValue("1");
    assertThat(testError).hasValue(null);
    assertThat(testEnd).hasValue(-1);
  }

  @Test
  public void cancel() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor, 1);
    Awaitable<String> awaitable = new Do<>(
        () -> new Return<>(new Iter<>("1", "2", "3").on(scheduler))
    ).on(scheduler);
    ArrayList<String> messages = new ArrayList<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicInteger testEnd = new AtomicInteger(-1);
    Cancelable cancelable =
        awaitable.await(-1, messages::add, testError::set, testEnd::set);
    lazyExecutor.advance(4);
    cancelable.cancel();
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(messages).containsExactly("1");
    assertThat(testError).hasValue(null);
    assertThat(testEnd).hasValue(Awaiter.CANCELED);
  }

  @Test
  public void cancelAgain() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor, 1);
    Awaitable<String> awaitable = new Do<>(
        () -> new Return<>(new Iter<>("1", "2", "3").on(scheduler))
    ).on(scheduler);
    ArrayList<String> messages = new ArrayList<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicInteger testEnd = new AtomicInteger(-1);
    Cancelable cancelable =
        awaitable.await(-1, messages::add, testError::set, testEnd::set);
    lazyExecutor.advance(5);
    cancelable.cancel();
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(messages).containsExactly("1");
    assertThat(testError).hasValue(null);
    assertThat(testEnd).hasValue(Awaiter.CANCELED);
  }

  @Test
  public void abort() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor, 1);
    Awaitable<String> awaitable = new Do<>(
        () -> new Return<>(new Iter<>("1", "2", "3").on(scheduler))
    ).on(scheduler);
    ArrayList<String> messages = new ArrayList<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicInteger testEnd = new AtomicInteger(-1);
    awaitable.await(-1, messages::add, testError::set, testEnd::set);
    lazyExecutor.advance(4);
    awaitable.abort();
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(messages).containsExactly("1");
    assertThat(testError).hasValue(null);
    assertThat(testEnd).hasValue(Awaiter.ABORTED);
  }

  @Test
  public void abortAgain() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor, 1);
    Awaitable<String> awaitable = new Do<>(
        () -> new Return<>(new Iter<>("1", "2", "3").on(scheduler))
    ).on(scheduler);
    ArrayList<String> messages = new ArrayList<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicInteger testEnd = new AtomicInteger(-1);
    awaitable.await(-1, messages::add, testError::set, testEnd::set);
    lazyExecutor.advance(5);
    awaitable.abort();
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(messages).containsExactly("1");
    assertThat(testError).hasValue(null);
    assertThat(testEnd).hasValue(Awaiter.ABORTED);
  }
}
