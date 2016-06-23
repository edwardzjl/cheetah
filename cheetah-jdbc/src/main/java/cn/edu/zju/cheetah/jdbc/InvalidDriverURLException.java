/**
 * 
 */
package cn.edu.zju.cheetah.jdbc;

/**
 * @author JIANG
 *
 */
public class InvalidDriverURLException extends IllegalArgumentException {

  private static final long serialVersionUID = 3413051545214749366L;

  public InvalidDriverURLException() {
    super();
  }

  public InvalidDriverURLException(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidDriverURLException(String s) {
    super(s);
  }

  public InvalidDriverURLException(Throwable cause) {
    super(cause);
  }
  
}
