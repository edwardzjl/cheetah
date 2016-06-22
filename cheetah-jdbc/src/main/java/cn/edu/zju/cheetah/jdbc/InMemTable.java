/**
 * 
 */
package cn.edu.zju.cheetah.jdbc;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Append only in-memory table implementation.
 * 
 * @author JIANG
 *
 */
public class InMemTable implements Iterable<Tuple> {
  
  private TableSchema schema;

  private List<Tuple> storage;

  public InMemTable(TableSchema schema) {
    this(schema, new ArrayList<>());
  }
  
  public InMemTable(TableSchema schema, List<Tuple> storage) {
    this.schema = checkNotNull(schema);
    this.storage = checkNotNull(storage);
  }
  
  public TableSchema getSchema() {
    return this.schema;
  }
  
  public void append(Tuple tuple) {
    storage.add(checkNotNull(tuple));
  }
  
  public void append(Collection<? extends Tuple> tuples) {
    storage.addAll(tuples);
  }

  @Override
  public Iterator<Tuple> iterator() {
    return storage.iterator();
  }
 
}
