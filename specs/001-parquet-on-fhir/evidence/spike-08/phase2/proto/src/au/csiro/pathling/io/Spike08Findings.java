package au.csiro.pathling.io;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

/**
 * Spike 08 prototype (throwaway). The findings of one document under decision 133's policies: under
 * {@code fail} the first finding that is not informational throws, naming the resource, the path and
 * the value; under {@code drop} each is recorded and the value is left out. The path is held as a stack
 * of names and indices and formatted only when a finding is made, so a clean document pays nothing for it.
 */
final class Spike08Findings {

  final boolean fail;
  final String resourceType;
  String resourceId;
  final List<Row> found = new ArrayList<>();
  private final String[] names = new String[64];
  private final int[] indices = new int[64];
  private int depth;

  Spike08Findings(boolean fail, String resourceType) {
    this.fail = fail;
    this.resourceType = resourceType;
  }

  void reset() {
    found.clear();
    depth = 0;
    resourceId = null;
  }

  void push(String name) {
    names[depth] = name;
    indices[depth] = -1;
    depth++;
  }

  void index(int i) {
    indices[depth - 1] = i;
  }

  void pop() {
    depth--;
  }

  String path(String leaf) {
    final StringBuilder b = new StringBuilder(resourceType);
    for (int i = 0; i < depth; i++) {
      b.append('.').append(names[i]);
      if (indices[i] >= 0) {
        b.append('[').append(indices[i]).append(']');
      }
    }
    if (leaf != null) {
      b.append('.').append(leaf);
    }
    return b.toString();
  }

  /** Records a finding at the current path, with an optional leaf name below it. */
  void add(String leaf, String kind, String value) {
    final boolean info = "contained".equals(kind);
    final String path = path(leaf);
    if (fail && !info) {
      throw new IllegalArgumentException(
          "Invalid content in " + resourceType + "/" + resourceId + " at " + path + " (" + kind + "): "
              + truncate(value));
    }
    found.add(RowFactory.create(path, kind, truncate(value)));
  }

  static String truncate(String value) {
    return value == null ? null : value.length() > 200 ? value.substring(0, 200) + "..." : value;
  }
}
