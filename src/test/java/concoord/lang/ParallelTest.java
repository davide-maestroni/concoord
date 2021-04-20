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
import concoord.lang.Parallel.OutputStrategyFactory;
import concoord.lang.Parallel.SchedulingStrategyFactory;
import concoord.lang.Parallel.StreamingStrategyFactory;
import concoord.parallel.output.Ordered;
import concoord.parallel.output.Unordered;
import concoord.parallel.scheduling.LoadBalancing;
import concoord.parallel.scheduling.RoundRobin;
import concoord.parallel.streaming.Balanced;
import concoord.parallel.streaming.Each;
import concoord.parallel.streaming.Grouped;
import concoord.test.TestBasic;
import concoord.test.TestCancel;
import concoord.test.TestSuite;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.data.MapEntry;
import org.junit.jupiter.api.Test;

public class ParallelTest {

  @Test
  public void basic() {
    List<TestBasic<String>> tests = IntStream.range(-1, 5)
        .boxed()
        .flatMap(parallelism ->
            Stream.<SchedulingStrategyFactory<String>>of(
                () -> new RoundRobin<>(parallelism, Trampoline::new),
                () -> new LoadBalancing<>(parallelism, Trampoline::new)
            ).flatMap(schedulingStrategyFactory ->
                Stream.<Entry<StreamingStrategyFactory<String, String>, Boolean>>of(
                    MapEntry.entry(Each::new, true),
                    MapEntry.entry(Grouped::new, false),
                    MapEntry.entry(Balanced::new, false)
                ).flatMap(streamingStrategyFactory ->
                    Stream.<Entry<OutputStrategyFactory<String>, Boolean>>of(
                        MapEntry.entry(Ordered::new, true),
                        MapEntry.entry(Unordered::new, false)
                    ).flatMap(outputStrategyFactory ->
                        IntStream.range(-1, 5)
                            .filter(i -> i != 0)
                            .boxed()
                            .map(events ->
                                new TestBasic<>(
                                    String.format(Locale.ROOT, "basic - %d - %s - %s - %s - %d",
                                        parallelism,
                                        schedulingStrategyFactory.getClass().getSimpleName(),
                                        streamingStrategyFactory.getKey().getClass().getSimpleName(),
                                        outputStrategyFactory.getKey().getClass().getSimpleName(),
                                        events
                                    ),
                                    (scheduler) ->
                                        new Parallel<>(
                                            new Iter<>("1", "2", "3").on(scheduler),
                                            schedulingStrategyFactory,
                                            streamingStrategyFactory.getKey(),
                                            outputStrategyFactory.getKey(),
                                            (a, s) -> new For<>(events, a, (m) -> new Yield<>("N" + m, events)).on(s)
                                        ).on(scheduler),
                                    (messages) -> {
                                      if (streamingStrategyFactory.getValue() && outputStrategyFactory.getValue()) {
                                        assertThat(messages).containsExactly("N1", "N2", "N3");
                                      } else {
                                        assertThat(messages).containsExactlyInAnyOrder("N1", "N2", "N3");
                                      }
                                    }
                                )
                            )
                    )
                )
            )
        )
        .collect(Collectors.toList());
    new TestSuite(tests).run();
  }

  @Test
  public void cancel() {
    List<TestCancel<String>> tests = IntStream.range(-1, 5)
        .boxed()
        .flatMap(parallelism ->
            Stream.<SchedulingStrategyFactory<String>>of(
                () -> new RoundRobin<>(parallelism, Trampoline::new),
                () -> new LoadBalancing<>(parallelism, Trampoline::new)
            ).flatMap(schedulingStrategyFactory ->
                Stream.<Entry<StreamingStrategyFactory<String, String>, Boolean>>of(
                    MapEntry.entry(Each::new, true),
                    MapEntry.entry(Grouped::new, false),
                    MapEntry.entry(Balanced::new, false)
                ).flatMap(streamingStrategyFactory ->
                    Stream.<Entry<OutputStrategyFactory<String>, Boolean>>of(
                        MapEntry.entry(Ordered::new, true),
                        MapEntry.entry(Unordered::new, false)
                    ).flatMap(outputStrategyFactory ->
                        IntStream.range(-1, 5)
                            .filter(i -> i != 0)
                            .boxed()
                            .map(events ->
                                new TestCancel<>(
                                    String.format(Locale.ROOT, "cancel - %d - %s - %s - %s - %d",
                                        parallelism,
                                        schedulingStrategyFactory.getClass().getSimpleName(),
                                        streamingStrategyFactory.getKey().getClass().getSimpleName(),
                                        outputStrategyFactory.getKey().getClass().getSimpleName(),
                                        events
                                    ),
                                    (scheduler) ->
                                        new Parallel<>(
                                            new Iter<>("1", "2", "3").on(scheduler),
                                            schedulingStrategyFactory,
                                            streamingStrategyFactory.getKey(),
                                            outputStrategyFactory.getKey(),
                                            (a, s) -> new For<>(events, a, (m) -> new Yield<>("N" + m, events)).on(s)
                                        ).on(scheduler),
                                    (messages) -> {
                                      if (streamingStrategyFactory.getValue() && outputStrategyFactory.getValue()) {
                                        assertThat(messages).containsExactly("N1", "N2", "N3");
                                      } else {
                                        assertThat(messages).containsExactlyInAnyOrder("N1", "N2", "N3");
                                      }
                                    }
                                )
                            )
                    )
                )
            )
        )
        .collect(Collectors.toList());
    new TestSuite(tests).run();
  }
}
