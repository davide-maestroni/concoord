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
package concoord.logging;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

public class LoggerTest {

  @Test
  public void printHelloDbg() {
    TestPrinter printer = new TestPrinter();
    Logger logger = new Logger(LoggerTest.class);
    logger.addPrinter(printer);
    logger.log(new DbgMessage("hello"));
    assertThat(printer.getDbgMessages()).hasSize(1);
    assertThat(printer.getInfMessages()).isEmpty();
    assertThat(printer.getWrnMessages()).isEmpty();
    assertThat(printer.getErrMessages()).isEmpty();
    TestMessage testMessage = printer.getDbgMessages().get(0);
    assertThat(testMessage.getName()).isEqualTo(LoggerTest.class.getName());
    assertThat(testMessage.getMessage()).isEqualTo("hello");
    assertThat(testMessage.getError()).isNull();
  }

  @Test
  public void printHelloInf() {
    TestPrinter printer = new TestPrinter();
    Logger logger = new Logger(LoggerTest.class);
    logger.addPrinter(printer);
    logger.log(new InfMessage("hello"));
    assertThat(printer.getDbgMessages()).isEmpty();
    assertThat(printer.getInfMessages()).hasSize(1);
    assertThat(printer.getWrnMessages()).isEmpty();
    assertThat(printer.getErrMessages()).isEmpty();
    TestMessage testMessage = printer.getInfMessages().get(0);
    assertThat(testMessage.getName()).isEqualTo(LoggerTest.class.getName());
    assertThat(testMessage.getMessage()).isEqualTo("hello");
    assertThat(testMessage.getError()).isNull();
  }

  @Test
  public void printHelloWrn() {
    TestPrinter printer = new TestPrinter();
    Logger logger = new Logger(LoggerTest.class);
    logger.addPrinter(printer);
    logger.log(new WrnMessage("hello"));
    assertThat(printer.getDbgMessages()).isEmpty();
    assertThat(printer.getInfMessages()).isEmpty();
    assertThat(printer.getWrnMessages()).hasSize(1);
    assertThat(printer.getErrMessages()).isEmpty();
    TestMessage testMessage = printer.getWrnMessages().get(0);
    assertThat(testMessage.getName()).isEqualTo(LoggerTest.class.getName());
    assertThat(testMessage.getMessage()).isEqualTo("hello");
    assertThat(testMessage.getError()).isNull();
  }

  @Test
  public void printHelloErr() {
    TestPrinter printer = new TestPrinter();
    Logger logger = new Logger(LoggerTest.class);
    logger.addPrinter(printer);
    logger.log(new ErrMessage("hello"));
    assertThat(printer.getDbgMessages()).isEmpty();
    assertThat(printer.getInfMessages()).isEmpty();
    assertThat(printer.getWrnMessages()).isEmpty();
    assertThat(printer.getErrMessages()).hasSize(1);
    TestMessage testMessage = printer.getErrMessages().get(0);
    assertThat(testMessage.getName()).isEqualTo(LoggerTest.class.getName());
    assertThat(testMessage.getMessage()).isEqualTo("hello");
    assertThat(testMessage.getError()).isNull();
  }

  private static class TestMessage {

    private final String name;
    private final String message;
    private final Throwable error;

    public TestMessage(String name, String message, Throwable error) {
      this.name = name;
      this.message = message;
      this.error = error;
    }

    public String getName() {
      return name;
    }

    public String getMessage() {
      return message;
    }

    public Throwable getError() {
      return error;
    }
  }

  private static class TestPrinter implements LogPrinter {

    private final ArrayList<TestMessage> dbgMessages = new ArrayList<>();
    private final ArrayList<TestMessage> infMessages = new ArrayList<>();
    private final ArrayList<TestMessage> wrnMessages = new ArrayList<>();
    private final ArrayList<TestMessage> errMessages = new ArrayList<>();

    private boolean canPrintDbg = true;
    private boolean canPrintInf = true;
    private boolean canPrintWrn = true;
    private boolean canPrintErr = true;

    @Override
    public boolean canPrintDbg(@NotNull String name) {
      return canPrintDbg;
    }

    @Override
    public boolean canPrintInf(@NotNull String name) {
      return canPrintInf;
    }

    @Override
    public boolean canPrintWrn(@NotNull String name) {
      return canPrintWrn;
    }

    @Override
    public boolean canPrintErr(@NotNull String name) {
      return canPrintErr;
    }

    @Override
    public void printDbg(@NotNull String name, String message, Throwable error) {
      dbgMessages.add(new TestMessage(name, message, error));
    }

    @Override
    public void printInf(@NotNull String name, String message, Throwable error) {
      infMessages.add(new TestMessage(name, message, error));
    }

    @Override
    public void printWrn(@NotNull String name, String message, Throwable error) {
      wrnMessages.add(new TestMessage(name, message, error));
    }

    @Override
    public void printErr(@NotNull String name, String message, Throwable error) {
      errMessages.add(new TestMessage(name, message, error));
    }

    public void setCanPrintDbg(boolean canPrintDbg) {
      this.canPrintDbg = canPrintDbg;
    }

    public void setCanPrintInf(boolean canPrintInf) {
      this.canPrintInf = canPrintInf;
    }

    public void setCanPrintWrn(boolean canPrintWrn) {
      this.canPrintWrn = canPrintWrn;
    }

    public void setCanPrintErr(boolean canPrintErr) {
      this.canPrintErr = canPrintErr;
    }

    @NotNull
    public List<TestMessage> getDbgMessages() {
      return dbgMessages;
    }

    @NotNull
    public List<TestMessage> getInfMessages() {
      return infMessages;
    }

    @NotNull
    public List<TestMessage> getWrnMessages() {
      return wrnMessages;
    }

    @NotNull
    public List<TestMessage> getErrMessages() {
      return errMessages;
    }
  }
}
