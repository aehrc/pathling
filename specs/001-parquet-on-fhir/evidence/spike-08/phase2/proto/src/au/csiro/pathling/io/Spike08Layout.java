package au.csiro.pathling.io;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Spike 08 prototype (throwaway). The fitted layout of one structure: the fields the data populates,
 * in definition order, with their stored types (FR-011: a complex element appears only where some
 * descendant leaf does). Built from the definitions and the presence the fit pass collected, or from a
 * stored schema for decoding. Serializable, and finite, because presence is.
 */
final class Spike08Layout implements Serializable {

  private static final long serialVersionUID = 1L;

  static final StructType FINDING = new StructType()
      .add("path", DataTypes.StringType)
      .add("kind", DataTypes.StringType)
      .add("value", DataTypes.StringType);

  final Spike08Def def;
  final Spike08Def.Field[] fields;
  final Spike08Layout[] children;
  final Map<String, Integer> index = new HashMap<>();
  final StructType schema;
  /** The position of the _findings column at the root, or -1. */
  final int findings;

  private Spike08Layout(Spike08Def def, List<Spike08Def.Field> fields, List<Spike08Layout> children,
      StructType schema, int findings) {
    this.def = def;
    this.fields = fields.toArray(new Spike08Def.Field[0]);
    this.children = children.toArray(new Spike08Layout[0]);
    for (int i = 0; i < this.fields.length; i++) {
      if (this.fields[i] != null) {
        index.put(this.fields[i].name, i);
      }
    }
    this.schema = schema;
    this.findings = findings;
  }

  int size() {
    return fields.length + (findings >= 0 ? 1 : 0);
  }

  /** The layout of a resource from the paths the fit pass saw, or null where nothing is stored. */
  static Spike08Layout fitted(Spike08Def def, Set<String> presence, boolean findingsColumn) {
    return build(def, presence, "", findingsColumn);
  }

  private static Spike08Layout build(Spike08Def def, Set<String> presence, String prefix,
      boolean findingsColumn) {
    final List<Spike08Def.Field> fields = new ArrayList<>();
    final List<Spike08Layout> children = new ArrayList<>();
    StructType schema = new StructType();
    for (final Spike08Def.Field f : def.fields) {
      if (f.resourceType) {
        fields.add(f);
        children.add(null);
        schema = schema.add(f.name, DataTypes.StringType, true);
        continue;
      }
      final String path = prefix + f.name;
      if (f.prim != null) {
        if (presence.contains(path)) {
          fields.add(f);
          children.add(null);
          schema = schema.add(f.name, wrap(f, f.stored), true);
        }
      } else if (f.child != null && presence.contains(path)) {
        final Spike08Layout child = build(f.child, presence, path + "/", false);
        if (child != null) {
          fields.add(f);
          children.add(child);
          schema = schema.add(f.name, wrap(f, child.schema), true);
        }
      }
    }
    if (fields.isEmpty()) {
      return null;
    }
    int findings = -1;
    if (findingsColumn) {
      findings = fields.size();
      schema = schema.add("_findings", DataTypes.createArrayType(FINDING, true), true);
    }
    return new Spike08Layout(def, fields, children, schema, findings);
  }

  /** The layout of stored data, for decoding: the stored schema's fields, matched to the definitions. */
  static Spike08Layout stored(Spike08Def def, StructType stored) {
    final List<Spike08Def.Field> fields = new ArrayList<>();
    final List<Spike08Layout> children = new ArrayList<>();
    StructType schema = new StructType();
    for (final StructField sf : stored.fields()) {
      final Spike08Def.Field f = def.field(sf.name());
      if (f == null) {
        // A column the definitions do not describe (such as _findings) is not part of the resource.
        fields.add(null);
        children.add(null);
        schema = schema.add(sf);
        continue;
      }
      fields.add(f);
      final DataType element = sf.dataType() instanceof ArrayType a ? a.elementType() : sf.dataType();
      children.add(f.prim == null && element instanceof StructType s ? stored(f.child, s) : null);
      schema = schema.add(sf);
    }
    return new Spike08Layout(def, fields, children, schema, -1);
  }

  private static DataType wrap(Spike08Def.Field f, DataType t) {
    return f.repeating ? DataTypes.createArrayType(t, true) : t;
  }
}
