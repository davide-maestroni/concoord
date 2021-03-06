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

public class Return<T> implements Result<T> {

  private final Result<T> result;

  public Return(T output) {
    this.result = new ResultMessage<T>(output);
  }

  public Return(@NotNull Awaitable<? extends T> awaitable) {
    this.result = new ResultAwaitable<T>(awaitable);
  }

  public void apply(@NotNull FlowControl<? super T> flowControl) {
    result.apply(flowControl);
    flowControl.stop();
  }
}
