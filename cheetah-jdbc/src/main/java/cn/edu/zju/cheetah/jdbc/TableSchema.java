/**
 * 
 */
package cn.edu.zju.cheetah.jdbc;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The definition of a relational table
 * @author JIANG
 *
 */
public class TableSchema {
  
  private List<ColumnSchema> columns;
  
  private Map<String, Integer> colMap = new HashMap<>();
  
  public TableSchema() {
    this.columns = new ArrayList<>();
  }
  
  public TableSchema(List<ColumnSchema> columns) {
    this.columns = checkNotNull(columns);
    int i = 0;
    for(ColumnSchema column : columns) {
      colMap.put(column.getColumnName(), i++);
    }
  }
  
  public TableSchema addColumn(ColumnSchema colDef) {
    columns.add(checkNotNull(colDef));
    colMap.put(colDef.getColumnName(), columns.size() - 1);
    return this;
  }
  
  public List<ColumnSchema> getColumns() {
    return this.columns;
  }
  
  /**
   * Return the ordinal index of a given column, -1 if not found
   * @param colName
   * @return -1 if not found, otherwise the ordinal index
   */
  public int findColumn(String colName) {
    return colMap.get(colName) == null ? -1 : colMap.get(colName);
  }
  
  @Override
  public boolean equals(Object obj) {
    if(obj == null)
      return false;
    if(obj instanceof TableSchema) {
      TableSchema other = (TableSchema) obj;
      return Objects.equals(this, other);
    }
    return false;
  }

  @Override
  public String toString() {
    return columns.toString();
  }

}
