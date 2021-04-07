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

import concoord.concurrent.Trampoline;
import concoord.flow.Yield;
import concoord.scheduling.buffer.Ordered;
import concoord.scheduling.buffer.Unordered;
import concoord.scheduling.control.Each;
import concoord.scheduling.control.Partial;
import concoord.scheduling.strategy.LoadBalancing;
import concoord.scheduling.strategy.RoundRobin;
import concoord.test.TestBasic;
import concoord.test.TestCancel;
import org.junit.jupiter.api.Test;

public class ParallelTest {

  @Test
  public void basicEachOrderedRound() {
    new TestBasic<>(
        (scheduler) ->
            new Parallel<>(
                new Iter<>("1", "2", "3").on(scheduler),
                () -> new Each<>(
                    new RoundRobin<>(3, Trampoline::new),
                    (a, s) -> new For<>(a, (m) -> new Yield<>("N" + m, -1)).on(s)
                ),
                Ordered::new
            ).on(scheduler),
        (messages) -> assertThat(messages).containsExactly("N1", "N2", "N3")
    ).run();
  }

  @Test
  public void basicPartialOrderedRound() {
    new TestBasic<>(
        (scheduler) ->
            new Parallel<>(
                new Iter<>("1", "2", "3").on(scheduler),
                () -> new Partial<>(
                    new RoundRobin<>(3, Trampoline::new),
                    (a, s) -> new For<>(-1, a, (m) -> new Yield<>("N" + m, -1)).on(s)
                ),
                Ordered::new
            ).on(scheduler),
        (messages) -> assertThat(messages).containsExactly("N1", "N2", "N3")
    ).run();
  }

  @Test
  public void basicOrderedBalance() {
    new TestBasic<>(
        (scheduler) ->
            new Parallel<>(
                new Iter<>("1", "2", "3").on(scheduler),
                () -> new Each<>(
                    new LoadBalancing<>(3, Trampoline::new),
                    (a, s) -> new For<>(a, (m) -> new Yield<>("N" + m, -1)).on(s)
                ),
                Ordered::new
            ).on(scheduler),
        (messages) -> assertThat(messages).containsExactly("N1", "N2", "N3")
    ).run();
  }

  @Test
  public void basicUnorderedRound() {
    new TestBasic<>(
        (scheduler) ->
            new Parallel<>(
                new Iter<>("1", "2", "3").on(scheduler),
                () -> new Each<>(
                    new RoundRobin<>(3, Trampoline::new),
                    (a, s) -> new For<>(a, (m) -> new Yield<>("N" + m, -1)).on(s)
                ),
                Unordered::new
            ).on(scheduler),
        (messages) -> assertThat(messages).containsExactly("N1", "N2", "N3")
    ).run();
  }

  @Test
  public void basicUnorderedBalance() {
    new TestBasic<>(
        (scheduler) ->
            new Parallel<>(
                new Iter<>("1", "2", "3").on(scheduler),
                () -> new Each<>(
                    new LoadBalancing<>(3, Trampoline::new),
                    (a, s) -> new For<>(a, (m) -> new Yield<>("N" + m, -1)).on(s)
                ),
                Unordered::new
            ).on(scheduler),
        (messages) -> assertThat(messages).containsExactly("N1", "N2", "N3")
    ).run();
  }

  @Test
  public void cancelOrderedRound() {
    new TestCancel<>(
        (scheduler) ->
            new Parallel<>(
                new Iter<>("1", "2", "3").on(scheduler),
                () -> new Each<>(
                    new RoundRobin<>(3, Trampoline::new),
                    (a, s) -> new For<>(a, (m) -> new Yield<>("N" + m, -1)).on(s)
                ),
                Ordered::new
            ).on(scheduler),
        (messages) -> assertThat(messages).containsExactly("N1", "N2", "N3")
    ).run();
  }

  @Test
  public void cancelOrderedBalance() {
    new TestCancel<>(
        (scheduler) ->
            new Parallel<>(
                new Iter<>("1", "2", "3").on(scheduler),
                () -> new Each<>(
                    new LoadBalancing<>(3, Trampoline::new),
                    (a, s) -> new For<>(a, (m) -> new Yield<>("N" + m, -1)).on(s)
                ),
                Ordered::new
            ).on(scheduler),
        (messages) -> assertThat(messages).containsExactly("N1", "N2", "N3")
    ).run();
  }

  @Test
  public void cancelUnorderedRound() {
    new TestCancel<>(
        (scheduler) ->
            new Parallel<>(
                new Iter<>("1", "2", "3").on(scheduler),
                () -> new Each<>(
                    new RoundRobin<>(3, Trampoline::new),
                    (a, s) -> new For<>(a, (m) -> new Yield<>("N" + m, -1)).on(s)
                ),
                Unordered::new
            ).on(scheduler),
        (messages) -> assertThat(messages).containsExactly("N1", "N2", "N3")
    ).run();
  }

  @Test
  public void cancelUnorderedBalance() {
    new TestCancel<>(
        (scheduler) ->
            new Parallel<>(
                new Iter<>("1", "2", "3").on(scheduler),
                () -> new Each<>(
                    new LoadBalancing<>(3, Trampoline::new),
                    (a, s) -> new For<>(a, (m) -> new Yield<>("N" + m, -1)).on(s)
                ),
                Unordered::new
            ).on(scheduler),
        (messages) -> assertThat(messages).containsExactly("N1", "N2", "N3")
    ).run();
  }
}
