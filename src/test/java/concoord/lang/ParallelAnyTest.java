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
import concoord.concurrent.Trampoline;
import concoord.flow.Yield;
import concoord.scheduling.RoundRobin;
import concoord.test.TestCancel;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

public class ParallelAnyTest {

  @Test
  public void basic() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor);
    Awaitable<String> awaitable = new ParallelAny<>(
        new Iter<>("1", "2", "3").on(scheduler),
        () -> new RoundRobin<>(
            3,
            Trampoline::new,
            (s, a) -> new For<>(a, (m) -> new Yield<>("N" + m, Integer.MAX_VALUE)).on(s)
        )
    ).on(scheduler);
    ArrayList<String> testMessages = new ArrayList<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicBoolean testEnd = new AtomicBoolean();
    awaitable.await(-1, testMessages::add, testError::set, () -> testEnd.set(true));
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(testMessages).containsExactlyInAnyOrder("N1", "N2", "N3");
    assertThat(testError).hasValue(null);
    assertThat(testEnd).isTrue();
  }

  @Test
  public void cancel() {
    new TestCancel<>(
        (scheduler) ->
            new ParallelAny<>(
                new Iter<>("1", "2", "3").on(scheduler),
                () -> new RoundRobin<>(
                    3,
                    Trampoline::new,
                    (s, a) -> new For<>(a, (m) -> new Yield<>("N" + m, Integer.MAX_VALUE)).on(s)
                )
            ).on(scheduler),
        (messages) -> assertThat(messages).containsExactlyInAnyOrder("N1", "N2", "N3")
    ).run();
  }
}
