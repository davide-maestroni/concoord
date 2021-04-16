package concoord.lang;

import static org.assertj.core.api.Assertions.assertThat;

import concoord.concurrent.AbortException;
import concoord.concurrent.Awaitable;
import concoord.concurrent.CancelException;
import concoord.concurrent.Cancelable;
import concoord.concurrent.LazyExecutor;
import concoord.concurrent.ScheduledExecutor;
import concoord.flow.Return;
import concoord.flow.Yield;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
    AtomicBoolean testEnd = new AtomicBoolean();
    awaitable.await(Integer.MAX_VALUE, testMessage::set, testError::set, () -> testEnd.set(true));
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessage).hasValue("hello");
    assertThat(testError).hasValue(null);
    assertThat(testEnd).isTrue();
  }

  @Test
  public void noEnd() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor);
    Awaitable<String> awaitable = new Do<>(() -> new Return<>("hello")).on(scheduler);
    AtomicReference<String> testMessage = new AtomicReference<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicBoolean testEnd = new AtomicBoolean();
    awaitable.await(1, testMessage::set, testError::set, () -> testEnd.set(true));
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessage).hasValue("hello");
    assertThat(testError).hasValue(null);
    assertThat(testEnd).isFalse();
  }

  @Test
  public void multiAwaitLazy() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor);
    Awaitable<String> awaitable = new Do<>(() -> new Return<>("hello")).on(scheduler);
    AtomicReference<String> testMessage = new AtomicReference<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicInteger testEnd = new AtomicInteger();
    awaitable.await(1, testMessage::set, testError::set, testEnd::incrementAndGet);
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessage).hasValue("hello");
    assertThat(testError).hasValue(null);
    assertThat(testEnd).hasValue(0);

    testMessage.set(null);
    awaitable.await(1, testMessage::set, testError::set, testEnd::incrementAndGet);
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessage).hasValue(null);
    assertThat(testError).hasValue(null);
    assertThat(testEnd).hasValue(1);
  }

  @Test
  public void multiAwaitActive() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor);
    Awaitable<String> awaitable = new Do<>(() -> new Return<>("hello")).on(scheduler);
    AtomicReference<String> testMessage = new AtomicReference<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicInteger testEnd = new AtomicInteger();
    awaitable.await(1, testMessage::set, testError::set, testEnd::incrementAndGet);
    awaitable.await(1, testMessage::set, testError::set, testEnd::incrementAndGet);
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessage).hasValue("hello");
    assertThat(testError).hasValue(null);
    assertThat(testEnd).hasValue(1);
  }

  @Test
  public void endless() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor);
    Awaitable<String> awaitable = new Do<>(() -> new Yield<>("hello")).on(scheduler);
    ArrayList<String> testMessages = new ArrayList<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicBoolean testEnd = new AtomicBoolean();
    awaitable.await(11, testMessages::add, testError::set, () -> testEnd.set(true));
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessages).hasSize(11).containsOnly("hello");
    assertThat(testError).hasValue(null);
    assertThat(testEnd).isFalse();
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
    AtomicBoolean testEnd = new AtomicBoolean();
    awaitable.await(1, testMessage::set, testError::set, () -> testEnd.set(true));
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessage).hasValue("1");
    assertThat(testError).hasValue(null);
    assertThat(testEnd).isFalse();
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
    AtomicBoolean testEnd = new AtomicBoolean();
    Cancelable cancelable =
        awaitable.await(-1, messages::add, testError::set, () -> testEnd.set(true));
    lazyExecutor.advance(5);
    cancelable.cancel();
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(messages).containsExactly("1");
    assertThat(testError.get()).isExactlyInstanceOf(CancelException.class);
    assertThat(testEnd).isFalse();
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
    AtomicBoolean testEnd = new AtomicBoolean();
    Cancelable cancelable =
        awaitable.await(-1, messages::add, testError::set, () -> testEnd.set(true));
    lazyExecutor.advance(6);
    cancelable.cancel();
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(messages).containsExactly("1");
    assertThat(testError.get()).isExactlyInstanceOf(CancelException.class);
    assertThat(testEnd).isFalse();
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
    AtomicBoolean testEnd = new AtomicBoolean();
    awaitable.await(-1, messages::add, testError::set, () -> testEnd.set(true));
    lazyExecutor.advance(5);
    awaitable.abort();
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(messages).containsExactly("1");
    assertThat(testError.get()).isExactlyInstanceOf(AbortException.class);
    assertThat(testEnd).isFalse();
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
    AtomicBoolean testEnd = new AtomicBoolean();
    awaitable.await(-1, messages::add, testError::set, () -> testEnd.set(true));
    lazyExecutor.advance(6);
    awaitable.abort();
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(messages).containsExactly("1");
    assertThat(testError.get()).isExactlyInstanceOf(AbortException.class);
    assertThat(testEnd).isFalse();
  }
}
