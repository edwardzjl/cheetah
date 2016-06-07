/**
 * 
 */
package cn.edu.zju.cheetah.jdbc;

import static com.google.common.base.Preconditions.checkNotNull;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * @author David
 *
 */
public class ResultSets {
  
  /**
   * Convert the current row into a string
   * @param rs the result set containing the current row
   * @return the string representation
   * @throws SQLException 
   */
  public static String rowToString(ResultSet rs) throws SQLException {
    checkNotNull(rs);
    StringBuilder rowBuf = new StringBuilder("(");
    ResultSetMetaData rsmd = rs.getMetaData();
    int cnt = rsmd.getColumnCount();
    for(int i = 1; i <cnt; i++)
      rowBuf.append(rs.getString(i) +", ");
    rowBuf.append(rs.getString(cnt) + ")");
    return rowBuf.toString();
  }
  
  /**
   * Dump the ResultSet to a string
   * @param rs the ResultSet
   * @return the string representation
   * @throws SQLException 
   */
  public static String toString(ResultSet rs) throws SQLException {
    checkNotNull(rs);
    StringBuilder rows = new StringBuilder("[");
    while(rs.next()) {
      rows.append(rowToString(rs) + ", ");
    }
    String rowsStr = rows.toString();
    return rowsStr.substring(0, rowsStr.length() - ", ".length()) + "]";
  }

}
