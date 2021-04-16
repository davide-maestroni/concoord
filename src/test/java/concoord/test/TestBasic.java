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
package concoord.test;

import static org.assertj.core.api.Assertions.assertThat;

import concoord.concurrent.Awaitable;
import concoord.concurrent.LazyExecutor;
import concoord.concurrent.ScheduledExecutor;
import concoord.concurrent.Scheduler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;

public class TestBasic<T> implements TestRunnable {

  // TODO: 08/04/21 abort => end (Streamed?)

  private final String name;
  private final Function<Scheduler, Awaitable<T>> factory;
  private final Consumer<List<T>> assertion;

  public TestBasic(@NotNull Function<Scheduler, Awaitable<T>> awaitableFactory,
      @NotNull Consumer<List<T>> messageAssertion) {
    this("basic", awaitableFactory, messageAssertion);
  }

  public TestBasic(String name, @NotNull Function<Scheduler, Awaitable<T>> awaitableFactory,
      @NotNull Consumer<List<T>> messageAssertion) {
    this.name = name;
    this.factory = awaitableFactory;
    this.assertion = messageAssertion;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public void run() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor, 1);
    ArrayList<T> testMessages = new ArrayList<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicBoolean testEnd = new AtomicBoolean();
    Awaitable<T> awaitable = factory.apply(scheduler);
    awaitable.await(-1, testMessages::add, testError::set, () -> testEnd.set(true));
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertion.accept(testMessages);
    assertThat(testError).hasValue(null);
    assertThat(testEnd).isTrue();
  }
}
