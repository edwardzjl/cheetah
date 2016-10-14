package cn.edu.zju.cheetah.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

public class TestCase {

  public static void printResultSet(ResultSet rs) throws SQLException {
    int columnCount = rs.getMetaData().getColumnCount();

    for (int i = 1; i <= columnCount; i++) {
      System.out.print(rs.getMetaData().getColumnName(i));
      if (i < columnCount) System.out.print(" | ");
    }
    System.out.println();

    while (rs.next()) {
      for (int i = 1; i <= columnCount; i++) {
        System.out.print(rs.getString(i));
        if (i < columnCount) System.out.print(" | ");
      }
      System.out.println();
    }
    System.out.println();
  }
}
