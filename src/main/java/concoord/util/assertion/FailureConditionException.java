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
package concoord.util.assertion;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public class FailureConditionException extends RuntimeException {

  private final ArrayList<RuntimeException> exceptions;

  public FailureConditionException(@NotNull List<RuntimeException> exceptions) {
    this.exceptions = new ArrayList<RuntimeException>(exceptions);
  }

  public FailureConditionException(String message, @NotNull List<RuntimeException> exceptions) {
    super(message);
    this.exceptions = new ArrayList<RuntimeException>(exceptions);
  }

  @Override
  public void printStackTrace(PrintWriter writer) {
    super.printStackTrace(writer);
    writer.println("Caused by:");
    for (RuntimeException exception : exceptions) {
      exception.printStackTrace(writer);
    }
  }

  @Override
  public void printStackTrace(PrintStream stream) {
    super.printStackTrace(stream);
    stream.println("Caused by:");
    for (RuntimeException exception : exceptions) {
      exception.printStackTrace(stream);
    }
  }
}
