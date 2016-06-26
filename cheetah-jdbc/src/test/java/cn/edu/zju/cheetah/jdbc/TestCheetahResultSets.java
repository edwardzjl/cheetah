/**
 * 
 */
package cn.edu.zju.cheetah.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import junit.framework.TestCase;

/**
 * @author VcamX
 *
 */
public class TestCheetahResultSets extends TestCase {

  public void testInMemTable() throws SQLException {
    TableSchema schema = new TableSchema();
    schema.addColumn(new ColumnSchema("firstName", java.sql.Types.VARCHAR));
    schema.addColumn(new ColumnSchema("lastName", java.sql.Types.VARCHAR));

    InMemTable memTable = new InMemTable(schema);
    memTable.append(Tuple.of("david", "jiang"));
    memTable.append(Tuple.of("Wendy", "Xu"));

    ResultSet rs = new CheetahResultSet(memTable);
    System.out.println(ResultSets.toString(rs));
  }

  public void testGetColumnIndex() throws SQLException {
    TableSchema schema = new TableSchema();
    schema.addColumn(new ColumnSchema("value", java.sql.Types.VARCHAR));

    InMemTable memTable = new InMemTable(schema);
    memTable.append(Tuple.of("1987"));
    memTable.append(Tuple.of("1987.5"));

    ResultSet rs = new CheetahResultSet(memTable);
    assertEquals(true, rs.next());
    assertEquals("1987", rs.getString(1));
    assertEquals((short) 1987, rs.getShort(1));
    assertEquals(1987, rs.getInt(1));
    assertEquals(1987L, rs.getLong(1));
    assertEquals(true, rs.next());
    assertEquals(1987.5F, rs.getFloat(1));
    assertEquals(1987.5, rs.getDouble(1));
  }

  public void testGetColumnLabel() throws SQLException {
    TableSchema schema = new TableSchema();
    schema.addColumn(new ColumnSchema("value", java.sql.Types.VARCHAR));

    InMemTable memTable = new InMemTable(schema);
    memTable.append(Tuple.of("1987"));
    memTable.append(Tuple.of("1987.5"));

    ResultSet rs = new CheetahResultSet(memTable);
    assertEquals(true, rs.next());
    assertEquals("1987", rs.getString("value"));
    assertEquals((short) 1987, rs.getShort("value"));
    assertEquals(1987, rs.getInt("value"));
    assertEquals(1987L, rs.getLong("value"));
    assertEquals(true, rs.next());
    assertEquals(1987.5F, rs.getFloat("value"));
    assertEquals(1987.5, rs.getDouble("value"));
  }

}
