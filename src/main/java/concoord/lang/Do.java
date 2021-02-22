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
import concoord.flow.FlowControl;
import concoord.flow.NullaryInvocation;
import concoord.flow.Result;
import concoord.logging.ErrMessage;
import concoord.logging.LogMessage;
import concoord.logging.Logger;
import concoord.logging.SafeString;
import concoord.logging.WrnMessage;
import concoord.util.CircularQueue;
import concoord.util.assertion.IfNull;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.jetbrains.annotations.NotNull;

public class Do<T> implements Task<T> {

  private static final Object NULL = new Object();

  private final NullaryInvocation<T> invocation;

  public Do(@NotNull NullaryInvocation<T> invocation) {
    new IfNull(invocation, "invocation").throwException();
    this.invocation = invocation;
  }

  @NotNull
  public Awaitable<T> on(@NotNull Scheduler scheduler) {
    new IfNull(scheduler, "scheduler").throwException();
    return new DoAwaitable<T>(scheduler, invocation);
  }

  private static class DoAwaitable<T> implements Awaitable<T> {

    private final Logger logger = new Logger(Awaitable.class, this);
    private final CircularQueue<Awaitable<? extends T>> outputs = new CircularQueue<Awaitable<? extends T>>();
    private final ConcurrentLinkedQueue<Object> messages = new ConcurrentLinkedQueue<Object>();
    private final CircularQueue<DoFlowControl> flowControls = new CircularQueue<DoFlowControl>();
    private final Scheduler scheduler;
    private final NullaryInvocation<T> invocation;
    private DoFlowControl currentFlowControl;
    private boolean stopped;

    private DoAwaitable(@NotNull Scheduler scheduler, @NotNull NullaryInvocation<T> invocation) {
      this.scheduler = scheduler;
      this.invocation = invocation;
    }

    public void await(int maxEvents) {
      await(maxEvents, new DummyAwaiter<T>());
    }

    public void await(int maxEvents, @NotNull Awaiter<? super T> awaiter) {
      new IfNull(awaiter, "awaiter").throwException();
      scheduler.scheduleLow(new DoFlowControl(maxEvents, awaiter));
    }

    public void abort() {
      scheduler.scheduleHigh(new Runnable() {
        public void run() {
          stopped = true;
          outputs.clear();
        }
      });
    }

    private class DoFlowControl implements FlowControl<T>, Awaiter<T>, Runnable {

      private final Awaiter<? super T> awaiter;
      private int maxEvents = 1;
      private int events;
      private int posts;
      private boolean ended;

      private DoFlowControl(int maxEvents, @NotNull Awaiter<? super T> awaiter) {
        this.events = maxEvents;
        this.awaiter = awaiter;
      }

      public void postOutput(T message) {
        if (++posts > 1) {
          throw new IllegalStateException("multiple outputs posted by the result");
        }
        --events;
        try {
          awaiter.message(message);
        } catch (final Exception e) {
          sendError(e);
        }
      }

      public void postOutput(Awaitable<? extends T> awaitable) {
        if (++posts > 1) {
          throw new IllegalStateException("multiple outputs posted by the result");
        }
        if (awaitable != null) {
          outputs.add(awaitable);
        }
      }

      public void limitInputs(int maxEvents) {
        this.maxEvents = maxEvents;
      }

      public void stop() {
        stopped = true;
      }

      public void message(T message) throws Exception {
        messages.add(message != null ? message : NULL);
        scheduler.scheduleLow(this);
      }

      public void error(final Throwable error) throws Exception {
        scheduler.scheduleLow(new Runnable() {
          public void run() {
            sendError(error);
            scheduler.scheduleLow(currentFlowControl);
            currentFlowControl = null;
          }
        });
      }

      public void end() throws Exception {
        scheduler.scheduleLow(new Runnable() {
          public void run() {
            sendEnd();
            scheduler.scheduleLow(currentFlowControl);
            currentFlowControl = null;
          }
        });
      }

      public void run() {
        // TODO: 22/02/21 fix this mess!!
        if (currentFlowControl != null) {
          flowControls.add(this);
          return;
        }
        if (stopped) {
          if (!ended) {
            sendEnd();
          }
          if (flowControls.isEmpty()) {
            return;
          }
          currentFlowControl = flowControls.poll();
        } else {
          currentFlowControl = this;
        }
        final CircularQueue<Awaitable<? extends T>> outputs = DoAwaitable.this.outputs;
        if ((events > 0) && outputs.isEmpty()) {
          try {
            final Result<T> result = invocation.call();
            posts = 0;
            result.apply(this);
          } catch (final Exception e) {
            logger.log(new WrnMessage(new LogMessage("invocation failed with an exception"), e));
            sendError(e);
          }
        }
        if (!outputs.isEmpty()) {
          final Awaitable<? extends T> awaitable = outputs.peek();
          awaitable.await(Math.min(events, maxEvents), this);
        } else {
          if (stopped || ended) {
            if (!ended) {
              sendEnd();
            }
            currentFlowControl = null;
          }
          scheduler.scheduleLow(this);
        }
      }

      private void sendError(@NotNull Throwable throwable) {
        stopped = true;
        ended = true;
        outputs.clear();
        try {
          awaiter.error(throwable);
        } catch (final Exception e) {
          logger.log(new ErrMessage(
              new LogMessage("failed to notify error to awaiter: %s", new SafeString(awaiter)),
              e
          ));
        }
      }

      private void sendEnd() {
        ended = true;
        try {
          awaiter.end();
        } catch (final Exception e) {
          logger.log(new ErrMessage(
              new LogMessage("failed to notify end to awaiter: %s", new SafeString(awaiter)),
              e
          ));
          sendError(e);
        }
      }
    }
  }
}
