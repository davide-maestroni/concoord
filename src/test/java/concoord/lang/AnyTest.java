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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

public class AnyTest {

  @Test
  public void basic() {
    LazyExecutor lazyExecutor = new LazyExecutor();
    ScheduledExecutor scheduler = new ScheduledExecutor(lazyExecutor);
    @SuppressWarnings("unchecked")
    Awaitable<? extends Serializable> awaitable = new Any<>(
        new Iter<>("1", "2", "3").on(scheduler),
        new Iter<>(1, 2, 3).on(scheduler)
    ).on(scheduler);
    ArrayList<Serializable> messages = new ArrayList<>();
    AtomicReference<Throwable> testError = new AtomicReference<>();
    AtomicInteger testEnd = new AtomicInteger(-1);
    awaitable.await(-1, messages::add, testError::set, testEnd::set);
    lazyExecutor.advance(Integer.MAX_VALUE);
    assertThat(messages).containsExactly("1", 1, "2", 2, "3", 3);
  }
}
