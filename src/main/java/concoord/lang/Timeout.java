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
import concoord.lang.StandardAwaitable.ExecutionControl;
import concoord.lang.StandardAwaitable.StandardFlowControl;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;

public class Timeout implements Task<Long> {

  private static final Object timerMutex = new Object();
  private static Timer timer;

  private final ControlFactory controlFactory;

  public Timeout(@NotNull Date delay) {
    this(System.currentTimeMillis() - delay.getTime(), TimeUnit.MILLISECONDS);
  }

  public Timeout(long delay, @NotNull TimeUnit timeUnit) {
    controlFactory = new DelayControlFactory(timeUnit.toMillis(delay));
  }

  public Timeout(@NotNull Date delay, long period, @NotNull TimeUnit timeUnit) {
    this(System.currentTimeMillis() - delay.getTime(), timeUnit.toMillis(period),
        TimeUnit.MILLISECONDS);
  }

  public Timeout(long delay, long period, @NotNull TimeUnit timeUnit) {
    this(delay, timeUnit, period, timeUnit);
  }

  public Timeout(long delay, @NotNull TimeUnit delayUnit, long period,
      @NotNull TimeUnit periodUnit) {
    controlFactory = new PeriodControlFactory(
        delayUnit.toMillis(delay),
        periodUnit.toMillis(period)
    );
  }

  @NotNull
  private static Timer timer() {
    synchronized (timerMutex) {
      if (timer == null) {
        timer = new Timer("concoord-timeout-timer");
      }
    }
    return timer;
  }

  @NotNull
  public Awaitable<Long> on(@NotNull Scheduler scheduler) {
    return new StandardAwaitable<Long>(scheduler, controlFactory.create(scheduler));
  }

  private interface ControlFactory {

    @NotNull
    ExecutionControl<Long> create(@NotNull Scheduler scheduler);
  }

  private static class DelayControlFactory implements ControlFactory {

    private final long delayMillis;

    private DelayControlFactory(long delayMillis) {
      this.delayMillis = delayMillis;
    }

    @NotNull
    public ExecutionControl<Long> create(@NotNull Scheduler scheduler) {
      return new DelayControl(scheduler, delayMillis);
    }
  }

  private static class DelayControl extends TimerTask implements ExecutionControl<Long> {

    private final StartState startState = new StartState();
    private final Scheduler scheduler;
    private final long delayMillis;
    private ExecutionControl<Long> controlState = startState;
    private StandardFlowControl<Long> flowControl;
    private Long message;

    private DelayControl(@NotNull Scheduler scheduler, long delayMillis) {
      this.scheduler = scheduler;
      this.delayMillis = delayMillis;
    }

    public boolean executeBlock(@NotNull StandardFlowControl<Long> flowControl) throws Exception {
      this.flowControl = flowControl;
      return controlState.executeBlock(flowControl);
    }

    public void cancelExecution() throws Exception {
      controlState.cancelExecution();
    }

    public void abortExecution(@NotNull Throwable error) throws Exception {
      controlState.abortExecution(error);
    }

    public void run() {
      scheduler.scheduleLow(new MessageCommand());
    }

    private static class EndState implements ExecutionControl<Long> {

      public boolean executeBlock(@NotNull StandardFlowControl<Long> flowControl) throws Exception {
        flowControl.stop();
        return true;
      }

      public void cancelExecution() {
      }

      public void abortExecution(@NotNull Throwable error) {
      }
    }

    private class MessageCommand implements Runnable {

      private final long message = System.currentTimeMillis();

      public void run() {
        DelayControl.this.message = message;
        flowControl.execute();
      }
    }

    private class StartState implements ExecutionControl<Long> {

      public boolean executeBlock(@NotNull StandardFlowControl<Long> flowControl) throws Exception {
        controlState = new MessageState();
        timer().schedule(DelayControl.this, delayMillis);
        return false;
      }

      public void cancelExecution() {
      }

      public void abortExecution(@NotNull Throwable error) {
      }
    }

    private class MessageState implements ExecutionControl<Long> {

      public boolean executeBlock(@NotNull StandardFlowControl<Long> flowControl) throws Exception {
        final Long message = DelayControl.this.message;
        if (message != null) {
          controlState = new EndState();
          flowControl.postOutput(message);
          flowControl.stop();
          return true;
        }
        return false;
      }

      public void cancelExecution() {
        if (cancel()) {
          controlState = startState;
        }
      }

      public void abortExecution(@NotNull Throwable error) {
        cancel();
      }
    }
  }

  private static class PeriodControlFactory implements ControlFactory {

    private final long delayMillis;
    private final long periodMillis;

    private PeriodControlFactory(long delayMillis, long periodMillis) {
      this.delayMillis = delayMillis;
      this.periodMillis = periodMillis;
    }

    @NotNull
    public ExecutionControl<Long> create(@NotNull Scheduler scheduler) {
      return new PeriodControl(scheduler, delayMillis, periodMillis);
    }
  }

  private static class PeriodControl extends TimerTask implements ExecutionControl<Long> {

    private final ConcurrentLinkedQueue<Long> outputQueue = new ConcurrentLinkedQueue<Long>();
    private final MessageCommand messageCommand = new MessageCommand();
    private final StartState startState = new StartState();
    private final MessageState messageState = new MessageState();
    private final Scheduler scheduler;
    private final long delayMillis;
    private final long periodMillis;
    private ExecutionControl<Long> controlState = startState;
    private StandardFlowControl<Long> flowControl;

    private PeriodControl(@NotNull Scheduler scheduler, long delayMillis, long periodMillis) {
      this.scheduler = scheduler;
      this.delayMillis = delayMillis;
      this.periodMillis = periodMillis;
    }

    public boolean executeBlock(@NotNull StandardFlowControl<Long> flowControl) throws Exception {
      this.flowControl = flowControl;
      return controlState.executeBlock(flowControl);
    }

    public void cancelExecution() throws Exception {
      controlState.cancelExecution();
    }

    public void abortExecution(@NotNull Throwable error) throws Exception {
      controlState.abortExecution(error);
    }

    public void run() {
      outputQueue.offer(System.currentTimeMillis());
      scheduler.scheduleLow(messageCommand);
    }

    private class MessageCommand implements Runnable {

      public void run() {
        flowControl.execute();
      }
    }

    private class StartState implements ExecutionControl<Long> {

      public boolean executeBlock(@NotNull StandardFlowControl<Long> flowControl) throws Exception {
        controlState = messageState;
        timer().scheduleAtFixedRate(PeriodControl.this, delayMillis, periodMillis);
        return false;
      }

      public void cancelExecution() {
      }

      public void abortExecution(@NotNull Throwable error) {
      }
    }

    private class MessageState implements ExecutionControl<Long> {

      public boolean executeBlock(@NotNull StandardFlowControl<Long> flowControl) throws Exception {
        final Long message = outputQueue.poll();
        if (message != null) {
          flowControl.postOutput(message);
          return true;
        }
        return false;
      }

      public void cancelExecution() {
        if (cancel()) {
          controlState = startState;
        }
      }

      public void abortExecution(@NotNull Throwable error) {
        cancel();
      }
    }
  }
}
