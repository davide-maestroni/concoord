/*
 * Copyright 2021 Davide Maestroni
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package concoord.concurrent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import concoord.lang.Do;
import concoord.lang.Iter;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

public class JoinTest {

  @Test
  public void join() {
    Awaitable<String> awaitable = new Iter<>(Arrays.asList("a", "b", "c")).on(new Trampoline());
    assertThat(new Join<>(awaitable, 1, 1, TimeUnit.SECONDS).toList())
        .containsExactly("a", "b", "c");
  }

  @Test
  public void joinTotal() {
    Awaitable<String> awaitable = new Iter<>(Arrays.asList("a", "b", "c")).on(new Trampoline());
    assertThat(new Join<>(awaitable, 10, 1, 1, TimeUnit.SECONDS).toList())
        .containsExactly("a", "b", "c");
  }

  @Test
  public void joinRemove() {
    Awaitable<String> awaitable = new Iter<>(Arrays.asList("a", "b", "c")).on(new Trampoline());
    assertThatThrownBy(() -> new Join<>(awaitable, 1, 1, TimeUnit.SECONDS).iterator().remove())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void joinTotalRemove() {
    Awaitable<String> awaitable = new Iter<>(Arrays.asList("a", "b", "c")).on(new Trampoline());
    assertThatThrownBy(() -> new Join<>(awaitable, 10, 1, 1, TimeUnit.SECONDS).iterator().remove())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void joinException() {
    Awaitable<String> awaitable = new Do<String>(() -> {
      throw new ArithmeticException();
    }).on(new Trampoline());
    assertThatThrownBy(() -> new Join<>(awaitable, 1, 1, TimeUnit.SECONDS).toList())
        .isInstanceOf(JoinException.class)
        .hasCauseInstanceOf(ArithmeticException.class);
  }

  @Test
  public void joinTotalException() {
    Awaitable<String> awaitable = new Do<String>(() -> {
      throw new ArithmeticException();
    }).on(new Trampoline());
    assertThatThrownBy(() -> new Join<>(awaitable, 10, 1, 1, TimeUnit.SECONDS).toList())
        .isInstanceOf(JoinException.class)
        .hasCauseInstanceOf(ArithmeticException.class);
  }

  @Test
  public void joinTimeout() {
    DummyAwaitable<String> awaitable = new DummyAwaitable<>();
    assertThatThrownBy(() -> new Join<>(awaitable, 1, 1, TimeUnit.SECONDS).toList())
        .isInstanceOf(JoinTimeoutException.class);
  }

  @Test
  public void joinTotalTimeout() {
    DummyAwaitable<String> awaitable = new DummyAwaitable<>();
    assertThatThrownBy(() -> new Join<>(awaitable, 10, 1, 1, TimeUnit.SECONDS).toList())
        .isInstanceOf(JoinTimeoutException.class);
  }

  @Test
  public void joinInterrupt() throws InterruptedException {
    DummyAwaitable<String> awaitable = new DummyAwaitable<>();
    AtomicReference<Throwable> throwable = new AtomicReference<>();
    Thread thread = new Thread(() -> {
      try {
        new Join<>(awaitable, 1, 1, TimeUnit.MINUTES).toList();
      } catch (Throwable t) {
        throwable.set(t);
      }
    });
    thread.start();
    Thread.sleep(500);
    thread.interrupt();
    Thread.sleep(500);
    assertThat(throwable.get())
        .isInstanceOf(JoinException.class)
        .hasCauseInstanceOf(InterruptedException.class);
  }

  @Test
  public void joinTotalInterrupt() throws InterruptedException {
    DummyAwaitable<String> awaitable = new DummyAwaitable<>();
    AtomicReference<Throwable> throwable = new AtomicReference<>();
    Thread thread = new Thread(() -> {
      try {
        new Join<>(awaitable, 10, 1, 1, TimeUnit.MINUTES).toList();
      } catch (Throwable t) {
        throwable.set(t);
      }
    });
    thread.start();
    Thread.sleep(500);
    thread.interrupt();
    Thread.sleep(500);
    assertThat(throwable.get())
        .isInstanceOf(JoinException.class)
        .hasCauseInstanceOf(InterruptedException.class);
  }

  private static class DummyAwaitable<T> implements Awaitable<T> {

    @NotNull
    @Override
    public Cancelable await(int maxEvents) {
      return new DummyCancelable();
    }

    @NotNull
    @Override
    public Cancelable await(int maxEvents, @NotNull Awaiter<? super T> awaiter) {
      return new DummyCancelable();
    }

    @NotNull
    @Override
    public Cancelable await(int maxEvents, @NotNull UnaryAwaiter<? super T> messageAwaiter,
        @NotNull UnaryAwaiter<? super Throwable> errorAwaiter, @NotNull NullaryAwaiter endAwaiter) {
      return new DummyCancelable();
    }

    @Override
    public void abort() {
    }
  }

  private static class DummyCancelable implements Cancelable {

    @Override
    public boolean isError() {
      return false;
    }

    @Override
    public boolean isDone() {
      return false;
    }

    @Override
    public void cancel() {
    }
  }
}
