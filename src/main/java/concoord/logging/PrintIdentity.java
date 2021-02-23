package concoord.logging;

import org.jetbrains.annotations.Nullable;

public class PrintIdentity {

  private final Object instance;

  public PrintIdentity(@Nullable Object instance) {
    this.instance = instance;
  }

  @Override
  public String toString() {
    final Object instance = this.instance;
    return (instance != null) ? instance.getClass().getName() + "@" +
        Integer.toHexString(System.identityHashCode(instance)) : "null";
  }
}
