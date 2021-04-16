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
package concoord.test;

import concoord.util.logging.PrintIdentity;
import concoord.util.logging.PrintStackTrace;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.logging.Logger;
import org.jetbrains.annotations.NotNull;

public class TestSuite implements TestRunnable {

  private static final Logger logger = Logger.getLogger(TestSuite.class.getName());

  private final String name;
  private final Collection<? extends TestRunnable> tests;

  public TestSuite(@NotNull TestRunnable... tests) {
    this(null, tests);
  }

  public TestSuite(@NotNull Collection<? extends TestRunnable> tests) {
    this(null, tests);
  }

  public TestSuite(String name, @NotNull TestRunnable... tests) {
    this(name, Arrays.asList(tests));
  }

  public TestSuite(String name, @NotNull Collection<? extends TestRunnable> tests) {
    this.name = name;
    this.tests = tests;
  }

  @NotNull
  private static String getCallerMethodName(@NotNull String callerOfMethodName) {
    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
    int length = stackTrace.length;
    for (int i = 0; i < length; ++i) {
      final StackTraceElement traceElement = stackTrace[i];
      if (TestSuite.class.getName().equals(traceElement.getClassName())
          && Objects.equals(callerOfMethodName, traceElement.getMethodName())) {
        if ((i + 1) < length) {
          return buildMethodName(stackTrace[i + 1]);
        }
        return "";
      }
    }
    return "";
  }

  @NotNull
  private static String buildMethodName(@NotNull StackTraceElement traceElement) {
    String[] parts = traceElement.getClassName().split("\\.");
    return parts[parts.length - 1] + "#" + traceElement.getMethodName();
  }

  private static void printReports(String suiteName, int successCount, int failureCount, List<TestReport> reports) {
    StringBuilder builder = new StringBuilder();
    for (TestReport report : reports) {
      builder.append(report).append('\n');
    }
    String report = String.format(Locale.ROOT, "%s =>\nSUCCESS: %d/%d\nFAILURE: %d/%d\n\n%s",
        suiteName, successCount, reports.size(), failureCount, reports.size(), builder);
    logger.info(report);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public void run() {
    int successCount = 0;
    int failureCount = 0;
    ArrayList<TestReport> reports = new ArrayList<>(tests.size());
    for (TestRunnable test : tests) {
      long start = System.currentTimeMillis();
      Object testName = test.name();
      if (testName == null) {
        testName = new PrintIdentity(test);
      }
      try {
        test.run();
        ++successCount;
        reports.add(new TestReport(testName, System.currentTimeMillis() - start));
      } catch (Exception e) {
        ++failureCount;
        reports.add(new TestReport(testName, System.currentTimeMillis() - start, e));
      }
    }
    String suiteName = name;
    if (suiteName == null) {
      suiteName = getCallerMethodName("run");
    }
    printReports(suiteName, successCount, failureCount, reports);
    if (failureCount > 0) {
      throw new AssertionError("test failed!");
    }
  }

  private static class TestReport {

    private final String format;
    private final Object[] args;

    private TestReport(@NotNull Object name, long millis) {
      this.format = "[SUCCESS] %s in %.3f";
      this.args = new Object[]{name, (float) millis / 1000};
    }

    private TestReport(@NotNull Object name, long millis, @NotNull Exception ex) {
      this.format = "[FAILURE] %s in %.3f with:\n%s";
      this.args = new Object[]{name, (float) millis / 1000, new PrintStackTrace(ex)};
    }

    @Override
    public String toString() {
      return String.format(Locale.ROOT, format, args);
    }
  }
}
