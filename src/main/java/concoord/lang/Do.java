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
import concoord.flow.Result;
import concoord.lang.BaseAwaitable.BaseFlowControl;
import concoord.lang.BaseAwaitable.ExecutionControl;
import concoord.util.assertion.IfNull;
import concoord.util.logging.DbgMessage;
import concoord.util.logging.PrintIdentity;
import org.jetbrains.annotations.NotNull;

public class Do<T> implements Task<T> {

  private final Block<T> block;

  public Do(@NotNull Block<T> block) {
    new IfNull("block", block).throwException();
    this.block = block;
  }

  @NotNull
  public Awaitable<T> on(@NotNull Scheduler scheduler) {
    return new BaseAwaitable<T>(scheduler, new DoControl<T>(block));
  }

  public interface Block<T> {

    @NotNull
    Result<T> execute() throws Exception;
  }

  private static class DoControl<T> implements ExecutionControl<T> {

    private final Block<T> block;

    private DoControl(@NotNull Block<T> block) {
      this.block = block;
    }

    public boolean executeBlock(@NotNull BaseFlowControl<T> flowControl) throws Exception {
      flowControl.logger().log(new DbgMessage("[executing] block: %s", new PrintIdentity(block)));
      block.execute().apply(flowControl);
      return true;
    }

    public void cancelExecution() {
    }

    public void abortExecution(@NotNull Throwable error) {
    }
  }
}
