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
import concoord.concurrent.Scheduler;
import concoord.concurrent.Trampoline;
import concoord.flow.Yield;
import concoord.lang.Parallel.Block;
import concoord.scheduling.BalancedInput;
import concoord.scheduling.Ordered;
import concoord.scheduling.Unordered;
import concoord.scheduling.strategy.LoadBalancing;
import concoord.scheduling.strategy.RoundRobin;
import concoord.test.TestBasic;
import concoord.test.TestCancel;
import concoord.test.TestRunnable;
import concoord.test.TestSuite;
import java.util.ArrayList;
import java.util.stream.IntStream;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

public class ParallelTest {

  @Test
  public void basicOrderedRound() {
    ArrayList<TestRunnable> tests = new ArrayList<>();
    IntStream.range(-1, 4)
        .mapToObj(parallelism ->
            new TestBasic<>(
                "basic infinite: " + parallelism,
                (scheduler) ->
                    new Parallel<>(
                        new Iter<>("1", "2", "3").on(scheduler),
                        () -> new Ordered<>(parallelism, new RoundRobin<>(), Trampoline::new),
                        (a, s) -> new For<>(-1, a, (m) -> new Yield<>("N" + m, -1)).on(s)
                    ).on(scheduler),
                (messages) -> assertThat(messages).containsExactly("N1", "N2", "N3")
            ))
        .forEach(tests::add);
    IntStream.range(-1, 4)
        .mapToObj(parallelism ->
            new TestBasic<>(
                "basic single: " + parallelism,
                (scheduler) ->
                    new Parallel<>(
                        new Iter<>("1", "2", "3").on(scheduler),
                        () -> new Ordered<>(parallelism, new RoundRobin<>(), Trampoline::new),
                        (a, s) -> new For<>(a, (m) -> new Yield<>("N" + m)).on(s)
                    ).on(scheduler),
                (messages) -> assertThat(messages).containsExactly("N1", "N2", "N3")
            ))
        .forEach(tests::add);
    new TestSuite(tests).run();
  }

  @Test
  public void basicOrderedBalance() {
    IntStream.range(-1, 4)
        .forEach(parallelism ->
            new TestBasic<>(
                (scheduler) ->
                    new Parallel<>(
                        new Iter<>("1", "2", "3").on(scheduler),
                        () -> new Ordered<>(parallelism, new LoadBalancing<>(), Trampoline::new),
                        (a, s) -> new For<>(-1, a, (m) -> new Yield<>("N" + m, -1)).on(s)
                    ).on(scheduler),
                (messages) -> assertThat(messages).containsExactly("N1", "N2", "N3")
            ).run());
    IntStream.range(-1, 4)
        .forEach(parallelism ->
            new TestBasic<>(
                (scheduler) ->
                    new Parallel<>(
                        new Iter<>("1", "2", "3").on(scheduler),
                        () -> new Ordered<>(parallelism, new LoadBalancing<>(), Trampoline::new),
                        (a, s) -> new For<>(a, (m) -> new Yield<>("N" + m)).on(s)
                    ).on(scheduler),
                (messages) -> assertThat(messages).containsExactly("N1", "N2", "N3")
            ).run());
  }

  @Test
  public void basicUnorderedRound() {
    IntStream.range(-1, 4)
        .forEach(parallelism ->
            new TestBasic<>(
                (scheduler) ->
                    new Parallel<>(
                        new Iter<>("1", "2", "3").on(scheduler),
                        () -> new Unordered<>(parallelism, new RoundRobin<>(), Trampoline::new),
                        (a, s) -> new For<>(-1, a, (m) -> new Yield<>("N" + m, -1)).on(s)
                    ).on(scheduler),
                (messages) -> assertThat(messages).containsExactlyInAnyOrder("N1", "N2", "N3")
            ).run());
    IntStream.range(-1, 4)
        .forEach(parallelism ->
            new TestBasic<>(
                (scheduler) ->
                    new Parallel<>(
                        new Iter<>("1", "2", "3").on(scheduler),
                        () -> new Unordered<>(parallelism, new RoundRobin<>(), Trampoline::new),
                        (a, s) -> new For<>(a, (m) -> new Yield<>("N" + m)).on(s)
                    ).on(scheduler),
                (messages) -> assertThat(messages).containsExactlyInAnyOrder("N1", "N2", "N3")
            ).run());
  }

  @Test
  public void basicUnorderedBalance() {
    IntStream.range(-1, 4)
        .forEach(parallelism ->
            new TestBasic<>(
                (scheduler) ->
                    new Parallel<>(
                        new Iter<>("1", "2", "3").on(scheduler),
                        () -> new Unordered<>(parallelism, new LoadBalancing<>(), Trampoline::new),
                        (a, s) -> new For<>(-1, a, (m) -> new Yield<>("N" + m, -1)).on(s)
                    ).on(scheduler),
                (messages) -> assertThat(messages).containsExactlyInAnyOrder("N1", "N2", "N3")
            ).run());
    IntStream.range(-1, 4)
        .forEach(parallelism ->
            new TestBasic<>(
                (scheduler) ->
                    new Parallel<>(
                        new Iter<>("1", "2", "3").on(scheduler),
                        () -> new Unordered<>(parallelism, new LoadBalancing<>(), Trampoline::new),
                        (a, s) -> new For<>(a, (m) -> new Yield<>("N" + m)).on(s)
                    ).on(scheduler),
                (messages) -> assertThat(messages).containsExactlyInAnyOrder("N1", "N2", "N3")
            ).run());
  }

  @Test
  public void basicBalancedInput() {
    IntStream.range(-1, 4)
        .forEach(parallelism ->
            new TestBasic<>(
                (scheduler) ->
                    new Parallel<>(
                        new Iter<>("1", "2", "3").on(scheduler),
                        () -> new BalancedInput<>(parallelism, Trampoline::new),
                        (a, s) -> new For<>(-1, a, (m) -> new Yield<>("N" + m, -1)).on(s)
                    ).on(scheduler),
                (messages) -> assertThat(messages).containsExactlyInAnyOrder("N1", "N2", "N3")
            ).run());
    IntStream.range(-1, 4)
        .forEach(parallelism ->
            new TestBasic<>(
                (scheduler) ->
                    new Parallel<>(
                        new Iter<>("1", "2", "3").on(scheduler),
                        () -> new BalancedInput<>(parallelism, Trampoline::new),
                        (a, s) -> new For<>(a, (m) -> new Yield<>("N" + m)).on(s)
                    ).on(scheduler),
                (messages) -> assertThat(messages).containsExactlyInAnyOrder("N1", "N2", "N3")
            ).run());
    IntStream.range(-1, 4)
        .forEach(parallelism ->
            new TestBasic<>(
                (scheduler) ->
                    new Parallel<>(
                        new Iter<>("1", "2", "3").on(scheduler),
                        () -> new BalancedInput<>(parallelism, Trampoline::new),
                        new Block<String, String>() {

                          private int count = 0;

                          @NotNull
                          @Override
                          public Awaitable<String> execute(@NotNull Awaitable<? extends String> a,
                              @NotNull Scheduler s) {
                            int myCount = ++count;
                            return new For<>(myCount, a, (m) -> new Yield<>("N" + m, myCount)).on(s);
                          }
                        }
                    ).on(scheduler),
                (messages) -> assertThat(messages).containsExactlyInAnyOrder("N1", "N2", "N3")
            ).run());
  }

  @Test
  public void cancelOrderedRound() {
    IntStream.range(-1, 4)
        .forEach(parallelism ->
            new TestCancel<>(
                (scheduler) ->
                    new Parallel<>(
                        new Iter<>("1", "2", "3").on(scheduler),
                        () -> new Ordered<>(parallelism, new RoundRobin<>(), Trampoline::new),
                        (a, s) -> new For<>(-1, a, (m) -> new Yield<>("N" + m, -1)).on(s)
                    ).on(scheduler),
                (messages) -> assertThat(messages).containsExactly("N1", "N2", "N3")
            ).run());
    IntStream.range(-1, 4)
        .forEach(parallelism ->
            new TestCancel<>(
                (scheduler) ->
                    new Parallel<>(
                        new Iter<>("1", "2", "3").on(scheduler),
                        () -> new Ordered<>(parallelism, new RoundRobin<>(), Trampoline::new),
                        (a, s) -> new For<>(a, (m) -> new Yield<>("N" + m)).on(s)
                    ).on(scheduler),
                (messages) -> assertThat(messages).containsExactly("N1", "N2", "N3")
            ).run());
  }

  @Test
  public void cancelOrderedBalance() {
    IntStream.range(-1, 4)
        .forEach(parallelism ->
            new TestCancel<>(
                (scheduler) ->
                    new Parallel<>(
                        new Iter<>("1", "2", "3").on(scheduler),
                        () -> new Ordered<>(parallelism, new LoadBalancing<>(), Trampoline::new),
                        (a, s) -> new For<>(-1, a, (m) -> new Yield<>("N" + m, -1)).on(s)
                    ).on(scheduler),
                (messages) -> assertThat(messages).containsExactly("N1", "N2", "N3")
            ).run());
    IntStream.range(-1, 4)
        .forEach(parallelism ->
            new TestCancel<>(
                (scheduler) ->
                    new Parallel<>(
                        new Iter<>("1", "2", "3").on(scheduler),
                        () -> new Ordered<>(parallelism, new LoadBalancing<>(), Trampoline::new),
                        (a, s) -> new For<>(a, (m) -> new Yield<>("N" + m)).on(s)
                    ).on(scheduler),
                (messages) -> assertThat(messages).containsExactly("N1", "N2", "N3")
            ).run());
  }

  @Test
  public void cancelUnorderedRound() {
    IntStream.range(-1, 4)
        .forEach(parallelism ->
            new TestCancel<>(
                (scheduler) ->
                    new Parallel<>(
                        new Iter<>("1", "2", "3").on(scheduler),
                        () -> new Unordered<>(parallelism, new RoundRobin<>(), Trampoline::new),
                        (a, s) -> new For<>(-1, a, (m) -> new Yield<>("N" + m, -1)).on(s)
                    ).on(scheduler),
                (messages) -> assertThat(messages).containsExactlyInAnyOrder("N1", "N2", "N3")
            ).run());
    IntStream.range(-1, 4)
        .forEach(parallelism ->
            new TestCancel<>(
                (scheduler) ->
                    new Parallel<>(
                        new Iter<>("1", "2", "3").on(scheduler),
                        () -> new Unordered<>(parallelism, new RoundRobin<>(), Trampoline::new),
                        (a, s) -> new For<>(a, (m) -> new Yield<>("N" + m)).on(s)
                    ).on(scheduler),
                (messages) -> assertThat(messages).containsExactlyInAnyOrder("N1", "N2", "N3")
            ).run());
  }

  @Test
  public void cancelUnorderedBalance() {
    IntStream.range(-1, 4)
        .forEach(parallelism ->
            new TestCancel<>(
                (scheduler) ->
                    new Parallel<>(
                        new Iter<>("1", "2", "3").on(scheduler),
                        () -> new Unordered<>(parallelism, new LoadBalancing<>(), Trampoline::new),
                        (a, s) -> new For<>(-1, a, (m) -> new Yield<>("N" + m, -1)).on(s)
                    ).on(scheduler),
                (messages) -> assertThat(messages).containsExactlyInAnyOrder("N1", "N2", "N3")
            ).run());
    IntStream.range(-1, 4)
        .forEach(parallelism ->
            new TestCancel<>(
                (scheduler) ->
                    new Parallel<>(
                        new Iter<>("1", "2", "3").on(scheduler),
                        () -> new Unordered<>(parallelism, new LoadBalancing<>(), Trampoline::new),
                        (a, s) -> new For<>(a, (m) -> new Yield<>("N" + m)).on(s)
                    ).on(scheduler),
                (messages) -> assertThat(messages).containsExactlyInAnyOrder("N1", "N2", "N3")
            ).run());
  }
}
