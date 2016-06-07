/**
 * 
 */
package cn.edu.zju.cheetah.sandbox.david;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author David
 *
 */
public class TestDerbyDML {

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    String dbUrl = "jdbc:derby:memory:demo;create=true";
    String sql = "CREATE TABLE Address (ID INT, StreetName VARCHAR(20), City VARCHAR(20))";
    try (Connection dbConn = DriverManager.getConnection(dbUrl)) {
      Statement stmt = dbConn.createStatement();
      System.out.println(stmt.execute(sql));
      DatabaseMetaData dmd = dbConn.getMetaData();
      System.out.println("JDBC Major Version: " + dmd.getJDBCMajorVersion());
      System.out.println("JDBC Minor Version: " + dmd.getJDBCMinorVersion());
      System.out.println(dmd.getSchemaTerm());
      try(ResultSet schemas = dmd.getSchemas()) {
        while(schemas.next()) {
          System.out.println("Schema: " + schemas.getString(1) + ", " + schemas.getString(2));
        }
      }
      try(ResultSet columns = dmd.getColumns(null, null, "ADDRESS", null)) {
        while(columns.next()) {
          System.out.println("Has a column");
          String row = null;
          for(int i = 0; i < 14; i++)
            row += columns.getString(i + 1) + ", ";
          row += columns.getString(15);
          System.out.println("Column: " + row);
        }
      }
      try(ResultSet tblTypes = dmd.getTableTypes()) {
        while(tblTypes.next()) {
          System.out.println("Table Type: " + tblTypes.getString(1));
        }
      }
      try(ResultSet tables = dmd.getTables(null, null, null, new String[] {"TABLE"})) {
        while(tables.next()) {
          StringBuilder builder = new StringBuilder();
          for(int i = 0; i < 9; i++)
            builder.append(tables.getString(i+1) + ", ");
          builder.append(tables.getString(10));
          System.out.println("Table: " + builder.toString());
          String tblName = tables.getString("TABLE_NAME");
          if(tblName.equals("ADDRESS")) {
            System.out.println("FK!");
          }
          if(tblName.equals("Address")) {
            System.out.println("FK Address!");
          }

          try(ResultSet rs = dmd.getColumns(null, null, tblName, null)) {
            while(rs.next()) {
              String row = null;
              for(int i = 0; i < 14; i++)
                row += rs.getString(i + 1) + ", ";
              row += rs.getString(15);
              System.out.println("Column: " + row);
            }
          }
        }
      }
    }
  }

}
