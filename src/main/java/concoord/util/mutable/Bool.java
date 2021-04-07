package concoord.util.mutable;

public class Bool extends Number {

  private boolean value;

  public Bool() {
  }

  public Bool(boolean value) {
    this.value = value;
  }

  public int intValue() {
    return value ? 1 : 0;
  }

  public long longValue() {
    return intValue();
  }

  public float floatValue() {
    return intValue();
  }

  public double doubleValue() {
    return intValue();
  }

  public boolean getValue() {
    return value;
  }

  public void setValue(boolean value) {
    this.value = value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Bool)) {
      return false;
    }
    final Bool other = (Bool) o;
    return value == other.getValue();
  }

  @Override
  public int hashCode() {
    return intValue();
  }

  @Override
  public String toString() {
    return java.lang.Boolean.toString(value);
  }
}
