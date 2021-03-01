package concoord.lang;

import static org.assertj.core.api.Assertions.assertThat;

import concoord.concurrent.Awaitable;
import concoord.concurrent.LazyExecutor;
import concoord.concurrent.ScheduledExecutor;
import concoord.flow.Yield;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

public class ForTest {

  @Test
  public void basic() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor);
    Awaitable<String> awaitable = new For<>(
        new Iter<>("1", "2", "3").on(scheduler),
        (m) -> new Yield<>("N" + m, Integer.MAX_VALUE)
    ).on(scheduler);
    ArrayList<String> testMessages = new ArrayList<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicBoolean testEnd = new AtomicBoolean();
    awaitable.await(Integer.MAX_VALUE, testMessages::add, testError::set, () -> testEnd.set(true));
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessages).containsExactly("N1", "N2", "N3");
    assertThat(testError.get()).isNull();
    assertThat(testEnd.get()).isTrue();
  }

  @Test
  public void abort() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor);
    Awaitable<String> awaitable = new For<>(
        new Iter<>("1", "2", "3").on(scheduler),
        (m) -> new Yield<>("N" + m, Integer.MAX_VALUE)
    ).on(scheduler);
    ArrayList<String> testMessages = new ArrayList<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicBoolean testEnd = new AtomicBoolean();
    awaitable.await(Integer.MAX_VALUE, testMessages::add, testError::set, () -> testEnd.set(true));
    awaitable.abort();
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessages).isEmpty();
    assertThat(testError.get()).isNull();
    assertThat(testEnd.get()).isTrue();
  }
}
