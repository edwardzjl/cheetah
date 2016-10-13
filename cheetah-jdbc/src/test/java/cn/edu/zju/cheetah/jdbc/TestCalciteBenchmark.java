package cn.edu.zju.cheetah.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.util.ConversionUtil;

import junit.framework.TestCase;

public class TestCalciteBenchmark extends TestCase {

  private static final String BROKER = "http://10.214.208.42:8082";
  private static final String COORDINATOR = "http://10.214.208.42:8081";

  private static final String[] SQLS_IMPLEMENTED = new String[]{
      // Case 1: select
      "SELECT \"good_name\" "
          + "FROM \"uniq_submit_order\"",

      // Case 2: select with where
      "SELECT COUNT(*) "
          + "FROM \"uniq_submit_order\" "
          + "WHERE \"fourth_category_name\" = 'DHA' AND \"clienttype\" = 'WEB'",

      // Case 3: select with aggregation
      "SELECT SUM(\"quantity\") AS quantity_sum "
          + "FROM \"uniq_submit_order\" "
          + "WHERE \"second_category_id\" <= 300",

      // Case 4: select with group by
      "SELECT \"fourth_category_name\", COUNT(*) AS total_count "
          + "FROM \"uniq_submit_order\" "
          + "GROUP BY \"fourth_category_name\"",

      // Case 5: select with where and aggregation
      "SELECT COUNT(*) AS total_count "
          + "FROM \"uniq_submit_order\" "
          + "WHERE \"second_category_id\" = 381",

      // Case 6: select with group and aggregation
      "SELECT \"clienttype\" AS client, SUM(\"quantity\") "
          + "FROM \"uniq_submit_order\" "
          + "GROUP BY \"clienttype\"",

      // Case 7: select with where, group and aggregation
      "SELECT \"clienttype\" AS client, SUM(\"quantity\") "
          + "FROM \"uniq_submit_order\" "
          + "WHERE \"fourth_category_name\" = 'DHA' "
          + "GROUP BY \"clienttype\"",

      // Case 8: select with limit
      "SELECT \"good_name\", \"quantity\" "
          + "FROM \"uniq_submit_order\" "
          + "LIMIT 5",

      // Case 10: select with where and limit
      "SELECT \"good_name\", \"quantity\" "
          + "FROM \"uniq_submit_order\" "
          + "WHERE \"fourth_category_name\" = 'DHA' "
          + "LIMIT 5",

      // Case 11: select with where and including non-ASCII literal
      "SELECT \"good_name\", \"quantity\", \"good_amount\" "
          + "FROM \"uniq_submit_order\" "
          + "WHERE \"fourth_category_name\" = '沐浴露'",
  };

  private static final String[] SQLS_PARTIAL_IMPLEMENTED = new String[]{
      // Case 1: select with group and having
      "SELECT COUNT(*) AS total_count "
          + "FROM \"uniq_submit_order\" "
          + "GROUP BY \"fourth_category_name\" "
          + "HAVING SUM(\"quantity\") > 10",

      // Case 2: select with order by
      "SELECT \"fourth_category_id\", COUNT(*) AS total_count "
          + "FROM \"uniq_submit_order\" AS t "
          + "GROUP BY \"fourth_category_id\" "
          + "ORDER BY \"fourth_category_id\" DESC",

      // Case 4: select with group by, order by and limit
      "SELECT \"fourth_category_id\", COUNT(*) AS total_count "
          + "FROM \"uniq_submit_order\" AS t "
          + "GROUP BY \"fourth_category_id\" "
          + "ORDER BY \"fourth_category_id\" DESC "
          + "LIMIT 10",
  };


  private CalciteConnection calciteConnection ;


  private void prepare() throws ClassNotFoundException, SQLException {
    Class.forName("org.apache.calcite.jdbc.Driver");

    System.setProperty("saffron.default.charset", ConversionUtil.NATIVE_UTF16_CHARSET_NAME);
    System.setProperty("saffron.default.nationalcharset", ConversionUtil.NATIVE_UTF16_CHARSET_NAME);
    System.setProperty("saffron.default.collation.name",
        ConversionUtil.NATIVE_UTF16_CHARSET_NAME + "$en_US");

    Connection connection = DriverManager.getConnection("jdbc:calcite:", new Properties());
    calciteConnection = connection.unwrap(CalciteConnection.class);

    Map<String, Object> operand = new HashMap<>();
    operand.put("url", BROKER);
    operand.put("coordinatorUrl", COORDINATOR);

    CheetahSchemaFactory schemaFactory = new CheetahSchemaFactory();
    Schema schema = schemaFactory.create(calciteConnection.getRootSchema(), "default", operand);

    calciteConnection.getRootSchema().add("default", schema);
    calciteConnection.setSchema("default");
  }

  private long query(String sql) throws SQLException {
    Statement stmt = calciteConnection.createStatement();

    long currTime = System.currentTimeMillis();
    ResultSet rs = stmt.executeQuery(sql);
    long time = System.currentTimeMillis() - currTime;

    int idx = 1;
    while (rs.next()) {
      idx += 1;
    }

    rs.close();
    stmt.close();

    return time;
  }

  private void run(String[] sqls) throws SQLException {
    for (int i = 0; i < sqls.length; i++) {
      System.out.println("Case " + (i+1) + ": " + sqls[i]);

      // For warming up
      for (int j = 0; j < 5; j++) query(sqls[i]);

      long timeAcc = 0;
      int execCount = 10;
      for (int j = 0; j < execCount; j++) {
        timeAcc += query(sqls[i]);
      }

      System.out.println("Time consumed: " + timeAcc/execCount + "ms");

      System.out.println();
    }
  }

  public void testCalciteJDBC() throws SQLException, ClassNotFoundException {
    prepare();

    System.out.println("========== For implemented features ==========\n");
    run(SQLS_IMPLEMENTED);
    System.out.println();

//    System.out.println("========== For partial implemented features ==========\n");
//    run(SQLS_PARTIAL_IMPLEMENTED);
//    System.out.println();
  }

}
