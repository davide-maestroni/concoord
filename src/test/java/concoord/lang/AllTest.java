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
import concoord.concurrent.Awaiter;
import concoord.concurrent.LazyExecutor;
import concoord.concurrent.ScheduledExecutor;
import concoord.test.TestCancel;
import concoord.util.collection.Tuple;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

public class AllTest {

  @Test
  public void basic() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor);
    Awaitable<Tuple<String, Integer, Object, Object, Object, Object, Object, Object, Object, Object>> awaitable =
        new All<>(
            new Iter<>("1", "2", "3").on(scheduler),
            new Iter<>(1, 2, 3).on(scheduler)
        ).on(scheduler);
    ArrayList<Tuple<String, Integer, Object, Object, Object, Object, Object, Object, Object, Object>> messages =
        new ArrayList<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicInteger testEnd = new AtomicInteger(-1);
    awaitable.await(-1, messages::add, testError::set, testEnd::set);
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(messages)
        .containsExactly(new Tuple<>("1", 1), new Tuple<>("2", 2), new Tuple<>("3", 3));
    assertThat(testError).hasValue(null);
    assertThat(testEnd).hasValue(Awaiter.DONE);
  }

  @Test
  public void cancel() {
    new TestCancel<>(
        (scheduler) ->
            new All<>(
                new Iter<>("1", "2", "3").on(scheduler),
                new Iter<>(1, 2, 3).on(scheduler)
            ).on(scheduler),
        (messages) -> assertThat(messages)
            .containsExactly(new Tuple<>("1", 1), new Tuple<>("2", 2), new Tuple<>("3", 3))
    ).run();
  }
}
