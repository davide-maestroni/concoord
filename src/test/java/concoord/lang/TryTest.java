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
package concoord.lang;

import static org.assertj.core.api.Assertions.assertThat;

import concoord.concurrent.Awaitable;
import concoord.concurrent.LazyExecutor;
import concoord.concurrent.ScheduledExecutor;
import concoord.flow.Return;
import concoord.flow.Yield;
import concoord.lang.Try.Catch;
import concoord.lang.Try.Finally;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

public class TryTest {

  @Test
  public void basic() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor);
    Awaitable<Integer> awaitable = new Try<>(
        new Iter<>(1, 2, 3).on(scheduler)
    ).on(scheduler);
    ArrayList<Integer> messages = new ArrayList<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicBoolean testEnd = new AtomicBoolean();
    awaitable.await(-1, messages::add, testError::set, () -> testEnd.set(true));
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(messages).containsExactly(1, 2, 3);
    assertThat(testError).hasValue(null);
    assertThat(testEnd).isTrue();
  }

  @Test
  public void caught() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor);
    Awaitable<Integer> awaitable = new Try<>(
        new Do<Integer>(() -> {
          throw new ArithmeticException();
        }).on(scheduler),
        new Catch<>(RuntimeException.class, e -> new Return<>(1))
    ).on(scheduler);
    ArrayList<Integer> messages = new ArrayList<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicBoolean testEnd = new AtomicBoolean();
    awaitable.await(-1, messages::add, testError::set, () -> testEnd.set(true));
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(messages).containsExactly(1);
    assertThat(testError).hasValue(null);
    assertThat(testEnd).isTrue();
  }

  @Test
  public void uncaught() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor);
    Awaitable<Integer> awaitable = new Try<>(
        new Do<Integer>(() -> {
          throw new ArithmeticException();
        }).on(scheduler),
        new Catch<>(NullPointerException.class, e -> new Return<>(1))
    ).on(scheduler);
    ArrayList<Integer> messages = new ArrayList<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicBoolean testEnd = new AtomicBoolean();
    awaitable.await(-1, messages::add, testError::set, () -> testEnd.set(true));
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(messages).isEmpty();
    assertThat(testError.get()).isExactlyInstanceOf(ArithmeticException.class);
    assertThat(testEnd).isFalse();
  }

  @Test
  public void finallyOutput() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor);
    Awaitable<Integer> awaitable = new Try<>(
        new Do<Integer>(() -> {
          throw new ArithmeticException();
        }).on(scheduler),
        new Catch<>(NullPointerException.class, e -> new Return<>(1)),
        new Finally<>(() -> new Return<>(3))
    ).on(scheduler);
    ArrayList<Integer> messages = new ArrayList<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicBoolean testEnd = new AtomicBoolean();
    awaitable.await(-1, messages::add, testError::set, () -> testEnd.set(true));
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(messages).containsExactly(3);
    assertThat(testError).hasValue(null);
    assertThat(testEnd).isTrue();
  }

  @Test
  public void finallyOutputReturn() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor);
    Awaitable<Integer> awaitable = new Try<>(
        new Do<Integer>(() -> {
          throw new ArithmeticException();
        }).on(scheduler),
        new Finally<>(() -> new Return<>(3)),
        new Catch<>(RuntimeException.class, e -> new Return<>(1))
    ).on(scheduler);
    ArrayList<Integer> messages = new ArrayList<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicBoolean testEnd = new AtomicBoolean();
    awaitable.await(-1, messages::add, testError::set, () -> testEnd.set(true));
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(messages).containsExactly(3);
    assertThat(testError).hasValue(null);
    assertThat(testEnd).isTrue();
  }

  @Test
  public void finallyOutputYield() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor);
    Awaitable<Integer> awaitable = new Try<>(
        new Do<Integer>(() -> {
          throw new ArithmeticException();
        }).on(scheduler),
        new Finally<>(() -> new Yield<>(3)),
        new Catch<>(RuntimeException.class, e -> new Return<>(1))
    ).on(scheduler);
    ArrayList<Integer> messages = new ArrayList<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicBoolean testEnd = new AtomicBoolean();
    awaitable.await(-1, messages::add, testError::set, () -> testEnd.set(true));
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(messages).containsExactly(1);
    assertThat(testError).hasValue(null);
    assertThat(testEnd).isTrue();
  }
}
