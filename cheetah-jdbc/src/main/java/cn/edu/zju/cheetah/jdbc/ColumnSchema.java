/**
 * 
 */
package cn.edu.zju.cheetah.jdbc;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Objects;

import com.google.common.base.MoreObjects;

/**
 * Column definition. TODO: change colType from int to enum
 * 
 * @author JIANG
 *
 */
public class ColumnSchema {

  private String colName;

  private int colType;

  public ColumnSchema(String colName, int colType) {
    this.colName = checkNotNull(colName);
    this.colType = colType;
  }

  public String getColumnName() {
    return this.colName;
  }

  public int getColumnType() {
    return this.colType;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof ColumnSchema))
      return false;
    ColumnSchema other = (ColumnSchema) obj;
    return Objects.equals(colName, other.colName) && 
        Objects.equals(colType, other.colType);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .addValue(colName)
        .addValue(colType).toString();
  }

}
