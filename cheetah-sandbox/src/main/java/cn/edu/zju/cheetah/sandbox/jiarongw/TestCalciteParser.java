package cn.edu.zju.cheetah.sandbox.jiarongw;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

/**
 * Created by VcamX on 7/6/16.
 */
public class TestCalciteParser {

  private static void parse(String sql) throws SqlParseException {
    SqlParser parser = SqlParser.create(sql);
    SqlNode root = parser.parseQuery();

    System.out.println("==============================");
    System.out.println("Before:");
    System.out.println();
    System.out.println(sql);
    System.out.println();
    System.out.println("Parsed:");
    System.out.println();
    System.out.println(root);
    System.out.println("==============================");
    System.out.println();
  }

  public static void main(String[] args) throws SqlParseException {
    String sql;

    sql = "SELECT t0.A AS d0,\n"
        + "  COUNT(0) AS m0\n"
        + "FROM mock_table_1 AS t0\n"
        + "GROUP BY t0.A\n"
        + "LIMIT 20001";
    parse(sql);


    sql = "SELECT t0.store_id AS d0,\n"
        + "  COUNT(0) AS m0\n"
        + "FROM sales_fact_1997 AS t0\n"
        + "GROUP BY t0.store_id\n"
        + "LIMIT 20001";
    parse(sql);


    sql = "SELECT 1 AS d0,\n"
        + "  'hi' AS d1,\n"
        + "  COUNT(0) AS m0\n"
        + "FROM mock_table_1 AS t0\n"
        + "LIMIT 20001";
    parse(sql);


    sql = "SELECT 1 AS d0,\n"
        + "  COUNT(0) AS m0\n"
        + "FROM sales_fact_1997 AS t0\n"
        + "LIMIT 20001";
    parse(sql);


    sql = "SELECT MIN(t.m0),\n"
        + "  MAX(t.m0)\n"
        + "FROM (\n"
        + "  SELECT t0.A AS d0,\n"
        + "    SUM(t0.X) AS m0\n"
        + "  FROM mock_table_1 AS t0\n"
        + "  GROUP BY t0.A\n"
        + ") AS t";
    parse(sql);


    sql = "SELECT MIN(t.m0),\n"
        + "  MAX(t.m0)\n"
        + "FROM (\n"
        + "  SELECT t0.store_id AS d0,\n"
        + "    SUM(t0.store_sales) AS m0\n"
        + "  FROM sales_fact_1997 AS t0\n"
        + "  GROUP BY t0.store_id\n"
        + ") AS t";
    parse(sql);

    sql = "SELECT t0.A AS d0,\n"
        + "  t0.B AS d1,\n"
        + "  t0.C AS d2,\n"
        + "  SUM(t0.X) AS m0\n"
        + "FROM mock_table_1 AS t0\n"
        + "GROUP BY t0.A,\n"
        + "  t0.B,\n"
        + "  t0.C\n"
        + "LIMIT 20001";
    parse(sql);

    sql = "SELECT 1 AS d0,\n"
        + "  'hi' AS d2,\n"
        + "  2 AS d1,\n"
        + "  COUNT(0) AS m0\n"
        + "FROM mock_table_1 AS t0\n"
        + "LIMIT 20001";
    parse(sql);

    sql = "SELECT t0.A AS d0,\n"
        + "  t0.B AS d1,\n"
        + "  t0.C AS d2,\n"
        + "  t0.D AS d3,\n"
        + "  t0.E AS d4,\n"
        + "  t0.F AS d5,\n"
        + "  SUM(t0.SA) AS m0,\n"
        + "  SUM(t0.SB) AS m1,\n"
        + "  SUM(t0.SC) AS m2,\n"
        + "  SUM(t0.SD) AS m3,\n"
        + "  SUM(t0.X) AS m4\n"
        + "FROM mock_table_1 AS t0\n"
        + "INNER JOIN (\n"
        + "  SELECT t0.C AS fd\n"
        + "  FROM mock_table_1 AS t0\n"
        + "  GROUP BY t0.C\n"
        + "  HAVING (SUM(t0.SC) = 0)\n"
        + ") AS p0u0\n"
        + "ON (t0.C = p0u0.fd)\n"
        + "INNER JOIN (\n"
        + "  SELECT t0.D AS fd,\n"
        + "    SUM(t0.SD) AS top\n"
        + "  FROM mock_table_1 AS t0\n"
        + "  GROUP BY t0.D\n"
        + "  ORDER BY top ASC\n"
        + "  LIMIT 1\n"
        + ") AS p0u1\n"
        + "ON (t0.D = p0u1.fd)\n"
        + "WHERE (t0.A IN ('a0') AND (t0.B LIKE 'b%') AND (t0.B LIKE '%0') AND (t0.B                                                                                        LIKE '%0%') AND (t0.B NOT LIKE '%a%'))\n"
        + "GROUP BY t0.A,\n"
        + "  t0.B,\n"
        + "  t0.C,\n"
        + "  t0.D,\n"
        + "  t0.E,\n"
        + "  t0.F\n"
        + "HAVING (SUM(t0.X) <= 100000000)\n"
        + "LIMIT 20001";
    parse(sql);

    sql = "SELECT t0.A AS d0,\n"
        + "  t0.T AS d1,\n"
        + "  SUM(t0.X) AS m0\n"
        + "FROM mock_table_1 AS t0\n"
        + "GROUP BY t0.A,\n"
        + "  t0.T\n"
        + "LIMIT 20001";
    parse(sql);

    sql = "SELECT *\n"
        + "FROM mock_table_1 AS t\n"
        + "LIMIT 10";
    parse(sql);

    sql = "SELECT *\n"
        + "FROM sales_fact_1997 AS t\n"
        + "LIMIT 10";
    parse(sql);

    sql = "SELECT t0.A AS f0,\n"
        + "  t0.B AS f1,\n"
        + "  t0.C AS f2,\n"
        + "  t0.D AS f3,\n"
        + "  t0.E AS f4,\n"
        + "  t0.F AS f5,\n"
        + "  t0.N AS f6,\n"
        + "  t0.NA AS f7,\n"
        + "  t0.NB AS f8,\n"
        + "  t0.T AS f15,\n"
        + "  t0.SA AS f9,\n"
        + "  t0.SB AS f10,\n"
        + "  t0.SC AS f11,\n"
        + "  t0.SD AS f12,\n"
        + "  t0.SE AS f13,\n"
        + "  t0.SF AS f14,\n"
        + "  t0.X AS f16,\n"
        + "  t0.Y AS f17\n"
        + "FROM mock_table_1 AS t0\n"
        + "LIMIT 10";
    parse(sql);

    sql = "SELECT t0.product_id AS f1,\n"
        + "  t0.time_id AS f6,\n"
        + "  t0.customer_id AS f0,\n"
        + "  t0.promotion_id AS f2,\n"
        + "  t0.store_id AS f4,\n"
        + "  t0.store_sales AS f5,\n"
        + "  t0.store_cost AS f3,\n"
        + "  t0.unit_sales AS f7\n"
        + "FROM sales_fact_1997 AS t0\n"
        + "LIMIT 10";
    parse(sql);
  }
}
