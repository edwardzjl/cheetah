/**
 * 
 */
package cn.edu.zju.cheetah.jdbc;

import junit.framework.TestCase;

import java.sql.DatabaseMetaData;

/**
 * @author David
 *
 */
public class TestCheetahDatabaseMetaData extends TestCase {
  
  public void testIsWrapperFor() throws Exception {
    DatabaseMetaData dmd = new CheetahDatabaseMetaData(null, null);
    assertTrue(dmd.isWrapperFor(DatabaseMetaData.class));
  }

}
