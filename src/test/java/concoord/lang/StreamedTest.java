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

import concoord.concurrent.LazyExecutor;
import concoord.concurrent.ScheduledExecutor;
import concoord.lang.Streamed.StreamedAwaitable;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

public class StreamedTest {

  @Test
  public void basic() throws Exception {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor);
    Streamed<Integer> streamed = new Streamed<>();
    ArrayList<Integer> testMessages = new ArrayList<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicInteger testEnd = new AtomicInteger();
    StreamedAwaitable<Integer> awaitable = streamed.on(scheduler);
    try (Closeable ignored = awaitable.asCloseable()) {
      awaitable.message(1);
      awaitable.message(2);
      awaitable.message(3);
    }
    awaitable.await(-1, testMessages::add, testError::set, testEnd::incrementAndGet);
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessages).containsExactly(1, 2, 3);
    assertThat(testError).hasValue(null);
    assertThat(testEnd).hasValue(1);

    testMessages.clear();
    testError.set(null);
    testEnd.set(0);
    awaitable = streamed.on(scheduler);
    awaitable.await(-1, testMessages::add, testError::set, testEnd::incrementAndGet);
    try (Closeable ignored = awaitable.asCloseable()) {
      awaitable.message(1);
      awaitable.message(2);
      awaitable.message(3);
    }
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessages).containsExactly(1, 2, 3);
    assertThat(testError).hasValue(null);
    assertThat(testEnd).hasValue(1);
  }

  @Test
  public void end() throws Exception {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor);
    Streamed<Integer> streamed = new Streamed<>();
    ArrayList<Integer> testMessages = new ArrayList<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicInteger testEnd = new AtomicInteger();
    StreamedAwaitable<Integer> awaitable = streamed.on(scheduler);
    awaitable.end();
    awaitable.await(-1, testMessages::add, testError::set, testEnd::incrementAndGet);
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessages).isEmpty();
    assertThat(testError).hasValue(null);
    assertThat(testEnd).hasValue(1);

    testEnd.set(0);
    awaitable.await(-1, testMessages::add, testError::set, testEnd::incrementAndGet);
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessages).isEmpty();
    assertThat(testError).hasValue(null);
    assertThat(testEnd).hasValue(1);

    awaitable.abort();
    testEnd.set(0);
    awaitable.await(-1, testMessages::add, testError::set, testEnd::incrementAndGet);
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessages).isEmpty();
    assertThat(testError).hasValue(null);
    assertThat(testEnd).hasValue(1);
  }
}
