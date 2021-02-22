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
package concoord.flow;

import concoord.concurrent.Awaitable;
import org.jetbrains.annotations.NotNull;

public class Yield<T> implements Result<T> {

  private final int maxEvents;
  private final Result<T> result;

  public Yield(T output) {
    this(1, output);
  }

  public Yield(int maxEvents, T output) {
    this.maxEvents = maxEvents;
    this.result = new ResultMessage<T>(output);
  }

  public Yield(@NotNull Awaitable<T> awaitable) {
    this(1, awaitable);
  }

  public Yield(int maxEvents, @NotNull Awaitable<T> awaitable) {
    this.maxEvents = maxEvents;
    this.result = new ResultAwaitable<T>(awaitable);
  }

  public void apply(@NotNull FlowControl<? super T> flowControl) {
    result.apply(flowControl);
    flowControl.limitInputs(maxEvents);
  }
}
