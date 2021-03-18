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
import concoord.concurrent.Awaiter;
import concoord.concurrent.Cancelable;
import concoord.concurrent.LazyExecutor;
import concoord.concurrent.ScheduledExecutor;
import concoord.concurrent.Scheduler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;

public class TestCancel<T> implements Runnable {

  private final Function<Scheduler, Awaitable<T>> factory;
  private final Consumer<List<T>> assertion;

  public TestCancel(@NotNull Function<Scheduler, Awaitable<T>> awaitableFactory,
      @NotNull Consumer<List<T>> messageAssertion) {
    this.factory = awaitableFactory;
    this.assertion = messageAssertion;
  }

  @Override
  public void run() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor, 1);
    ArrayList<T> messages = new ArrayList<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicInteger testEnd = new AtomicInteger(-1);
    int i = 0;
    int count;
    boolean done;
    do {
      messages.clear();
      testError.set(null);
      testEnd.set(-1);
      Awaitable<T> awaitable = factory.apply(scheduler);
      Cancelable cancelable =
          awaitable.await(-1, messages::add, testError::set, testEnd::set);
      count = lazyExecutor.advance(i++);
      done = cancelable.isDone();
      cancelable.cancel();
      lazyExecutor.advance(Integer.MAX_VALUE);
      assertThat(testError).hasValue(null);
      assertThat(testEnd).hasValue(done ? Awaiter.DONE : Awaiter.CANCELED);
      awaitable.await(-1, messages::add, testError::set, testEnd::set);
      lazyExecutor.advance(Integer.MAX_VALUE);
      assertion.accept(messages);
      assertThat(testError).hasValue(null);
      assertThat(testEnd).hasValue(Awaiter.DONE);
    } while (count == (i - 1));
  }
}
