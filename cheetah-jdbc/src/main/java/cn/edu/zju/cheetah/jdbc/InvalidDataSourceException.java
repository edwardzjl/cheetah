/**
 * 
 */
package cn.edu.zju.cheetah.jdbc;

/**
 * @author JIANG
 *
 */
public class InvalidDataSourceException extends IllegalArgumentException {

  private static final long serialVersionUID = 3413051545214749366L;

  public InvalidDataSourceException() {
    super();
  }

  public InvalidDataSourceException(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidDataSourceException(String s) {
    super(s);
  }

  public InvalidDataSourceException(Throwable cause) {
    super(cause);
  }
  
}
