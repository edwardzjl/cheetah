/**
 * 
 */
package cn.edu.zju.cheetah.jdbc;

import static com.google.common.base.Preconditions.checkNotNull;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import com.google.common.base.MoreObjects;
import com.yahoo.sql4d.sql4ddriver.DDataSource;

/**
 * The JDBC connection implementation for a Druid database.
 * @author David
 *
 */
public class CheetahConnection implements Connection {
  
  private DriverURL driverUrl;
  
  private Properties prop;
  
  private DDataSource druidDriver;
  
  public CheetahConnection(DriverURL driverUrl, Properties info) {
    this.driverUrl = checkNotNull(driverUrl);
    this.prop = checkNotNull(info);
    String cHost = info.getProperty(CheetahCluster.COORDINATOR_HOST);
    String value = info.getProperty(CheetahCluster.COORDINATOR_PORT);
    if(cHost == null || value == null)
      throw new IllegalArgumentException("No coordinator!");
    int cPort = Integer.parseInt(value);
    String bHost = info.getProperty(CheetahCluster.BROKER_HOST);
    value = info.getProperty(CheetahCluster.BROKER_PORT);
    if(bHost == null || value == null)
      throw new IllegalArgumentException("No broker!");
    int bPort = Integer.parseInt(value);
    
    String oHost = info.getProperty(CheetahCluster.OVERLOAD_HOST);
    value = info.getProperty(CheetahCluster.OVERLOAD_PORT);
    int oPort = Integer.parseInt(value);
    
    System.out.printf("%s:%d %s:%d %s:%d\n", bHost, bPort, cHost, cPort, oHost, oPort);
    druidDriver = new DDataSource(bHost, bPort, cHost, cPort, oHost, oPort);
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
  public Statement createStatement() throws SQLException {
    return new CheetahStatement(druidDriver);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commit() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void rollback() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws SQLException {
    // Druid use HTTP connection, no need to close
  }

  @Override
  public boolean isClosed() throws SQLException {
    return false;
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return new CheetahDatabaseMetaData(druidDriver);
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    // No-op
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return true;
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
  }

  @Override
  public String getCatalog() throws SQLException {
    return driverUrl.getDatabase();
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see java.sql.Connection#prepareStatement(java.lang.String, int, int)
   */
  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see java.sql.Connection#prepareCall(java.lang.String, int, int)
   */
  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see java.sql.Connection#getTypeMap()
   */
  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see java.sql.Connection#setTypeMap(java.util.Map)
   */
  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see java.sql.Connection#setHoldability(int)
   */
  @Override
  public void setHoldability(int holdability) throws SQLException {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see java.sql.Connection#getHoldability()
   */
  @Override
  public int getHoldability() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  /* (non-Javadoc)
   * @see java.sql.Connection#setSavepoint()
   */
  @Override
  public Savepoint setSavepoint() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see java.sql.Connection#setSavepoint(java.lang.String)
   */
  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see java.sql.Connection#rollback(java.sql.Savepoint)
   */
  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see java.sql.Connection#releaseSavepoint(java.sql.Savepoint)
   */
  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see java.sql.Connection#prepareStatement(java.lang.String, int, int, int)
   */
  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see java.sql.Connection#prepareCall(java.lang.String, int, int, int)
   */
  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see java.sql.Connection#prepareStatement(java.lang.String, int)
   */
  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see java.sql.Connection#prepareStatement(java.lang.String, java.lang.String[])
   */
  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see java.sql.Connection#createClob()
   */
  @Override
  public Clob createClob() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see java.sql.Connection#createBlob()
   */
  @Override
  public Blob createBlob() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public NClob createNClob() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    return null;
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    return false;
  }

  /* (non-Javadoc)
   * @see java.sql.Connection#setClientInfo(java.lang.String, java.lang.String)
   */
  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    // TODO Auto-generated method stub

  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public String getSchema() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("CheetahCluster", prop)
        .add("Database", driverUrl.getDatabase()).toString();
  }

  
}
