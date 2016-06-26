/**
 * 
 */
package cn.edu.zju.cheetah.jdbc;

import static com.google.common.base.Preconditions.checkNotNull;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.List;

import com.google.common.base.MoreObjects;
import com.yahoo.sql4d.sql4ddriver.DDataSource;
import com.yahoo.sql4d.sql4ddriver.Joiner4All;
import com.yahoo.sql4d.sql4ddriver.Mapper4All;

import scala.util.Either;

/**
 * @author David
 *
 */
public class CheetahStatement implements Statement {
  
  private DDataSource druidDriver;
  
  public CheetahStatement(DDataSource druidDriver) {
    this.druidDriver = checkNotNull(druidDriver);
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    //TODO: test the following code
    Either<String, Either<Joiner4All, Mapper4All>> result = druidDriver.query(sql, null);
    if (result.isLeft())
      throw new SQLException(result.left().get());

    Either<Joiner4All, Mapper4All> goodResult = result.right().get();

    List<String> fields;
    List<List<Object>> rows;
    if (goodResult.isLeft()) {
      fields = goodResult.left().get().baseFieldNames;
      rows = (List<List<Object>>) goodResult.left().get().baseAllRows.values();
    } else {
      fields = goodResult.right().get().baseFieldNames;
      rows = goodResult.right().get().baseAllRows;
    }

    TableSchema schema = new TableSchema();
    for (String field : fields) {
      // TODO: retrieve type of each field and map it to SQL type
      schema.addColumn(new ColumnSchema(field, java.sql.Types.VARCHAR));
    }
    InMemTable memTable = new InMemTable(schema);
    for (List<Object> row : rows) {
      memTable.append(Tuple.of(row.toArray()));
    }
    return new CheetahResultSet(memTable);
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws SQLException {
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  /* (non-Javadoc)
   * @see java.sql.Statement#setMaxFieldSize(int)
   */
  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public int getMaxRows() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  /* (non-Javadoc)
   * @see java.sql.Statement#setMaxRows(int)
   */
  @Override
  public void setMaxRows(int max) throws SQLException {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see java.sql.Statement#setEscapeProcessing(boolean)
   */
  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see java.sql.Statement#getQueryTimeout()
   */
  @Override
  public int getQueryTimeout() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  /* (non-Javadoc)
   * @see java.sql.Statement#setQueryTimeout(int)
   */
  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see java.sql.Statement#cancel()
   */
  @Override
  public void cancel() throws SQLException {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see java.sql.Statement#getWarnings()
   */
  @Override
  public SQLWarning getWarnings() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see java.sql.Statement#clearWarnings()
   */
  @Override
  public void clearWarnings() throws SQLException {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see java.sql.Statement#setCursorName(java.lang.String)
   */
  @Override
  public void setCursorName(String name) throws SQLException {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see java.sql.Statement#execute(java.lang.String)
   */
  @Override
  public boolean execute(String sql) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  /* (non-Javadoc)
   * @see java.sql.Statement#getResultSet()
   */
  @Override
  public ResultSet getResultSet() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see java.sql.Statement#getUpdateCount()
   */
  @Override
  public int getUpdateCount() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  /* (non-Javadoc)
   * @see java.sql.Statement#getMoreResults()
   */
  @Override
  public boolean getMoreResults() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  /* (non-Javadoc)
   * @see java.sql.Statement#setFetchDirection(int)
   */
  @Override
  public void setFetchDirection(int direction) throws SQLException {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see java.sql.Statement#getFetchDirection()
   */
  @Override
  public int getFetchDirection() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  /* (non-Javadoc)
   * @see java.sql.Statement#setFetchSize(int)
   */
  @Override
  public void setFetchSize(int rows) throws SQLException {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see java.sql.Statement#getFetchSize()
   */
  @Override
  public int getFetchSize() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  /* (non-Javadoc)
   * @see java.sql.Statement#getResultSetConcurrency()
   */
  @Override
  public int getResultSetConcurrency() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  /* (non-Javadoc)
   * @see java.sql.Statement#getResultSetType()
   */
  @Override
  public int getResultSetType() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  /* (non-Javadoc)
   * @see java.sql.Statement#addBatch(java.lang.String)
   */
  @Override
  public void addBatch(String sql) throws SQLException {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see java.sql.Statement#clearBatch()
   */
  @Override
  public void clearBatch() throws SQLException {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see java.sql.Statement#executeBatch()
   */
  @Override
  public int[] executeBatch() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see java.sql.Statement#getConnection()
   */
  @Override
  public Connection getConnection() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see java.sql.Statement#getMoreResults(int)
   */
  @Override
  public boolean getMoreResults(int current) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  /* (non-Javadoc)
   * @see java.sql.Statement#getGeneratedKeys()
   */
  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see java.sql.Statement#executeUpdate(java.lang.String, int)
   */
  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  /* (non-Javadoc)
   * @see java.sql.Statement#executeUpdate(java.lang.String, int[])
   */
  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  /* (non-Javadoc)
   * @see java.sql.Statement#executeUpdate(java.lang.String, java.lang.String[])
   */
  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  /* (non-Javadoc)
   * @see java.sql.Statement#execute(java.lang.String, int)
   */
  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  /* (non-Javadoc)
   * @see java.sql.Statement#execute(java.lang.String, int[])
   */
  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  /* (non-Javadoc)
   * @see java.sql.Statement#execute(java.lang.String, java.lang.String[])
   */
  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  /* (non-Javadoc)
   * @see java.sql.Statement#getResultSetHoldability()
   */
  @Override
  public int getResultSetHoldability() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  /* (non-Javadoc)
   * @see java.sql.Statement#isClosed()
   */
  @Override
  public boolean isClosed() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  /* (non-Javadoc)
   * @see java.sql.Statement#setPoolable(boolean)
   */
  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see java.sql.Statement#isPoolable()
   */
  @Override
  public boolean isPoolable() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  /* (non-Javadoc)
   * @see java.sql.Statement#closeOnCompletion()
   */
  @Override
  public void closeOnCompletion() throws SQLException {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see java.sql.Statement#isCloseOnCompletion()
   */
  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).toString();
  }
  
}
