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

import concoord.concurrent.Awaitable;
import concoord.concurrent.Scheduler;
import concoord.concurrent.Task;
import concoord.lang.BaseAwaitable.AwaitableFlowControl;
import concoord.lang.BaseAwaitable.ExecutionControl;
import concoord.util.assertion.IfNull;
import org.jetbrains.annotations.NotNull;

public class Reschedule<T> implements Task<T> {

  private final Awaitable<T> awaitable;

  public Reschedule(@NotNull Awaitable<T> awaitable) {
    new IfNull("awaitable", awaitable).throwException();
    this.awaitable = awaitable;
  }

  @NotNull
  public Awaitable<T> on(@NotNull Scheduler scheduler) {
    return new BaseAwaitable<T>(scheduler, new RescheduleControl<T>(awaitable));
  }

  private static class RescheduleControl<T> implements ExecutionControl<T> {

    private final Awaitable<T> awaitable;

    private RescheduleControl(@NotNull Awaitable<T> awaitable) {
      this.awaitable = awaitable;
    }

    public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) {
      flowControl.postOutput(awaitable);
      flowControl.stop();
      return true;
    }

    public void cancelExecution() {
    }

    public void abortExecution(@NotNull Throwable error) {
      awaitable.abort();
    }
  }
}
