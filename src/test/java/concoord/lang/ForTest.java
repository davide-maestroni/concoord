package concoord.lang;

import static org.assertj.core.api.Assertions.assertThat;

import concoord.concurrent.AbortException;
import concoord.concurrent.Awaitable;
import concoord.concurrent.LazyExecutor;
import concoord.concurrent.ScheduledExecutor;
import concoord.concurrent.Trampoline;
import concoord.flow.Continue;
import concoord.flow.Return;
import concoord.flow.Yield;
import concoord.test.TestCancel;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
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
    assertThat(testError).hasValue(null);
    assertThat(testEnd).isTrue();
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
    assertThat(testError.get()).isExactlyInstanceOf(AbortException.class);
    assertThat(testEnd).isFalse();
  }

  @Test
  public void cancel() {
    new TestCancel<>(
        (scheduler) -> new For<>(
            new Iter<>("1", "2", "3").on(scheduler),
            s -> new Yield<>(new Iter<>("n" + s).on(scheduler))
        ).on(scheduler),
        (messages) -> assertThat(messages).containsExactly("n1", "n2", "n3")
    ).run();
  }

  @Test
  public void perf() {
    long total = 0;
    int repetitions = 1000;
    int inputs = 10000;
    Trampoline trampoline = new Trampoline();
    for (int i = 0; i < repetitions; i++) {
      long startTime = System.currentTimeMillis();
      long[] sum = new long[1];
      new And<>(
          new For<>(
              -1,
              new Iter<>(IntStream.rangeClosed(1, inputs).boxed()::iterator).on(trampoline),
              a -> {
                sum[0] += (long) a * (long) a;
                return new Continue<>(-1);
              }
          ).on(trampoline),
          new Do<>(
              () -> new Return<>(Math.sqrt((float) sum[0] / inputs))
          ).on(trampoline)
      ).on(trampoline).await(-1);
      total += System.currentTimeMillis() - startTime;
    }
    System.out.println(total / repetitions);
  }
}
