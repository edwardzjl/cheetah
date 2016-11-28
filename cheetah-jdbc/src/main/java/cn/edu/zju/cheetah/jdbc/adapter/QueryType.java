package cn.edu.zju.cheetah.jdbc.adapter;

/** Type of Cheetah query. */
public enum QueryType {
  SELECT("select"),
  TOP_N("topN"),
  GROUP_BY("groupBy"),
  TIMESERIES("timeseries");

  private final String queryName;

  private QueryType(String queryName) {
    this.queryName = queryName;
  }

  public String getQueryName() {
    return this.queryName;
  }
}

// End QueryType.java
