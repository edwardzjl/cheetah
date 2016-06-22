/**
 * 
 */
package cn.edu.zju.cheetah.jdbc;

import java.sql.DatabaseMetaData;

import junit.framework.TestCase;

/**
 * @author David
 *
 */
public class TestCheetahDatabaseMetaData extends TestCase {
  
  public void testIsWrapperFor() throws Exception {
    DatabaseMetaData dmd = new CheetahDatabaseMetaData(null);
    assertTrue(dmd.isWrapperFor(DatabaseMetaData.class));
  }
}
