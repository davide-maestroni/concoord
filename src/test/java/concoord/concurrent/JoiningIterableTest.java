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

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

public class JoiningIterableTest {

  @Test
  public void join() {
    TestAwaitable<String> awaitable = new TestAwaitable<>(Arrays.asList("a", "b", "c"));
    assertThat(new JoiningIterable<>(awaitable, 1, 1, TimeUnit.SECONDS).toList()).containsExactly("a", "b", "c");
  }

  @Test
  public void joinTotal() {
    TestAwaitable<String> awaitable = new TestAwaitable<>(Arrays.asList("a", "b", "c"));
    assertThat(new JoiningIterable<>(awaitable, 10, 1, 1, TimeUnit.SECONDS).toList()).containsExactly("a", "b", "c");
  }

  private static class TestAwaitable<T> implements Awaitable<T> {

    private final ExecutorService service = Executors.newSingleThreadExecutor();
    private final Iterator<T> iterator;

    private TestAwaitable(@NotNull Iterable<T> values) {
      this.iterator = values.iterator();
    }

    public void await(int maxEvents) {
    }

    public void await(int maxEvents, @NotNull Awaiter<? super T> awaiter) {
      service.execute(() -> {
        try {
          for (int i = 0; i < maxEvents; ++i) {
            if (iterator.hasNext()) {
              awaiter.message(iterator.next());
            } else {
              awaiter.end();
            }
          }
        } catch (Exception e) {
          try {
            awaiter.error(e);
          } catch (Exception ex) {
            throw new RuntimeException(ex);
          }
        }
      });
    }
  }
}
