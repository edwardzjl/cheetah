/**
 * 
 */
package cn.edu.zju.cheetah.jdbc;

import junit.framework.TestCase;

/**
 * @author JIANG
 *
 */
public class TestColumnSchema extends TestCase {

  public void testColumnSchema() throws Exception {
    ColumnSchema col = new ColumnSchema("c1", java.sql.Types.VARCHAR);
    assertEquals("c1", col.getColumnName());
    assertEquals(java.sql.Types.VARCHAR, col.getColumnType());
    System.out.println(col);
  }
}
