/**
 * 
 */
package cn.edu.zju.cheetah.jdbc;

import junit.framework.TestCase;

/**
 * @author JIANG
 *
 */
public class TestTableSchema extends TestCase {

  public void testTableSchema() throws Exception {
    TableSchema tableSchema = new TableSchema();
    tableSchema.addColumn(new ColumnSchema("c1", java.sql.Types.VARCHAR));
    tableSchema.addColumn(new ColumnSchema("c2", java.sql.Types.INTEGER));
    assertEquals(0, tableSchema.findColumn("c1"));
    assertEquals(-1, tableSchema.findColumn("c3"));
    System.out.println(tableSchema);
  }
}
