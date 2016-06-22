/**
 * 
 */
package cn.edu.zju.cheetah.jdbc;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Arrays;

/**
 * The generic implementation of a database tuple. To be consistent with
 * JDBC, the column index starts at 1 not 0.
 * 
 * @author JIANG
 *
 */
public class Tuple {

  private Object[] fields;

  private Tuple(Object[] fields) {
    this.fields = checkNotNull(fields);
  }

  public static Tuple of(Object... fields) {
    return new Tuple(fields);
  }

  public Object getField(int i) {
    checkArgument(i >= 0 && i < fields.length);
    return fields[i];
  }

  public Object[] getFields() {
    return this.fields;
  }

  public int cadinality() {
    return fields.length;
  }

  @Override
  public String toString() {
    return Arrays.toString(fields).replace('[', '(').replace(']', ')');
  }
}
