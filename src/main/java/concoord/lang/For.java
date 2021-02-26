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
import concoord.concurrent.Awaiter;
import concoord.concurrent.Scheduler;
import concoord.concurrent.Task;
import concoord.flow.Result;
import concoord.logging.DbgMessage;
import concoord.logging.PrintIdentity;
import concoord.util.assertion.IfNull;
import concoord.util.assertion.IfSomeOf;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.jetbrains.annotations.NotNull;

public class For<T, M> implements Task<T> {

  private final Awaitable<M> awaitable;
  private final Block<T, ? super M> block;

  public For(@NotNull Awaitable<M> awaitable, @NotNull Block<T, ? super M> block) {
    new IfSomeOf(
        new IfNull(block, "awaitable"),
        new IfNull(block, "block")
    ).throwException();
    this.awaitable = awaitable;
    this.block = block;
  }

  @NotNull
  public Awaitable<T> on(@NotNull Scheduler scheduler) {
    new IfNull(scheduler, "scheduler").throwException();
    return new ForAwaitable<T, M>(scheduler, awaitable, block);
  }

  public interface Block<T, M> {

    @NotNull
    Result<T> call(M message) throws Exception;
  }

  private static class ForAwaitable<T, M> extends BaseAwaitable<T> {

    private static final Object NULL = new Object();

    private final ConcurrentLinkedQueue<Object> inputs = new ConcurrentLinkedQueue<Object>();
    private final Scheduler scheduler;
    private final Awaitable<M> awaitable;
    private final Block<T, ? super M> block;
    private boolean ended;

    private ForAwaitable(@NotNull Scheduler scheduler, @NotNull Awaitable<M> awaitable,
        @NotNull Block<T, ? super M> block) {
      super(scheduler);
      this.scheduler = scheduler;
      this.awaitable = awaitable;
      this.block = block;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception {
      final Object message = inputs.poll();
      if (message == null) {
        if (ended) {
          flowControl.stop();
        } else {
          scheduler.scheduleLow(new InputState(flowControl));
          return false;
        }
      } else {
        flowControl.logger().log(new DbgMessage("[executing] block: %s", new PrintIdentity(block)));
        block.call(message != NULL ? (M) message : null).apply(flowControl);
      }
      return true;
    }

    private class InputState implements Runnable, Awaiter<M> {

      private final AwaitableFlowControl<T> flowControl;

      private InputState(@NotNull AwaitableFlowControl<T> flowControl) {
        this.flowControl = flowControl;
      }

      public void run() {
        awaitable.await(Math.max(1, flowControl.inputEvents()), this);
      }

      public void message(M message) throws Exception {
        inputs.offer(message != null ? message : NULL);
        scheduler.scheduleLow(flowControl);
      }

      public void error(@NotNull Throwable error) throws Exception {
        flowControl.error(error);
      }

      public void end() {
        scheduler.scheduleLow(new Runnable() {
          public void run() {
            ended = true;
          }
        });
      }
    }
  }
}
