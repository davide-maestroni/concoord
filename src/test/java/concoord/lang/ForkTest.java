package concoord.lang;

import static org.assertj.core.api.Assertions.assertThat;

import concoord.concurrent.Awaitable;
import concoord.concurrent.LazyExecutor;
import concoord.concurrent.ScheduledExecutor;
import concoord.data.Buffered;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

public class ForkTest {

  @Test
  public void basic() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor);
    Fork<Integer> fork = new Fork<>(new Iter<>(1, 2, 3).on(scheduler), new Buffered<>());
    ArrayList<Integer> testMessages = new ArrayList<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicBoolean testEnd = new AtomicBoolean();
    Awaitable<Integer> awaitable = fork.on(scheduler);
    awaitable.await(-1, testMessages::add, testError::set, () -> testEnd.set(true));
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessages).containsExactly(1, 2, 3);
    assertThat(testError).hasValue(null);
    assertThat(testEnd).isTrue();

    testMessages.clear();
    testError.set(null);
    testEnd.set(false);
    awaitable.await(-1, testMessages::add, testError::set, () -> testEnd.set(true));
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessages).isEmpty();
    assertThat(testError).hasValue(null);
    assertThat(testEnd).isTrue();

    testMessages.clear();
    testError.set(null);
    testEnd.set(false);
    fork.on(scheduler).await(-1, testMessages::add, testError::set, () -> testEnd.set(true));
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessages).containsExactly(1, 2, 3);
    assertThat(testError).hasValue(null);
    assertThat(testEnd).isTrue();
  }
}
