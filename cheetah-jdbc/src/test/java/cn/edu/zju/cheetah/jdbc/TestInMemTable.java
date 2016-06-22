/**
 * 
 */
package cn.edu.zju.cheetah.jdbc;

import java.sql.ResultSet;

import junit.framework.TestCase;

/**
 * @author JIANG
 *
 */
public class TestInMemTable extends TestCase {
  
  public void testInMemTable() throws Exception {
    TableSchema schema = new TableSchema();
    schema.addColumn(new ColumnSchema("firstName", java.sql.Types.VARCHAR));
    schema.addColumn(new ColumnSchema("lastName", java.sql.Types.VARCHAR));
    InMemTable memTable = new InMemTable(schema);
    memTable.append(Tuple.of("david", "jiang"));
    memTable.append(Tuple.of("Wendy", "Xu"));
    for(Tuple t : memTable)
      System.out.println(t);
    
    ResultSet rs = new CheetahResultSet(memTable);
    System.out.println(ResultSets.toString(rs));
  }

}
