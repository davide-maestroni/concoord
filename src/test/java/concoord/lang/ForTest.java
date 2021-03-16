package concoord.lang;

import static org.assertj.core.api.Assertions.assertThat;

import concoord.concurrent.Awaitable;
import concoord.concurrent.Awaiter;
import concoord.concurrent.LazyExecutor;
import concoord.concurrent.ScheduledExecutor;
import concoord.flow.Yield;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
    AtomicInteger testEnd = new AtomicInteger(-1);
    awaitable.await(Integer.MAX_VALUE, testMessages::add, testError::set, testEnd::set);
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessages).containsExactly("N1", "N2", "N3");
    assertThat(testError).hasValue(null);
    assertThat(testEnd).hasValue(Awaiter.DONE);
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
    AtomicInteger testEnd = new AtomicInteger(-1);
    awaitable.await(Integer.MAX_VALUE, testMessages::add, testError::set, testEnd::set);
    awaitable.abort();
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessages).isEmpty();
    assertThat(testError).hasValue(null);
    assertThat(testEnd).hasValue(Awaiter.ABORTED);
  }
}
