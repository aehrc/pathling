package au.csiro.pathling.io;

import au.csiro.pathling.definition.DefinitionContext;
import au.csiro.pathling.schema.DefinitionCanonicalStructure;
import au.csiro.pathling.schema.LayoutEntry;
import au.csiro.pathling.schema.PrimitiveTypes;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.types.DataType;

/**
 * Spike 08 prototype (throwaway). A node of the definitions as the B parser walks them: the fields of
 * one structure in definition order, each with its cardinality, primitive type and child node. Built
 * once on the driver from {@link DefinitionCanonicalStructure}; a type reached again is the same node,
 * so the graph is finite and may be cyclic, which Java serialisation carries to the executors.
 */
final class Spike08Def implements Serializable {

  private static final long serialVersionUID = 1L;

  /** One field of a node. */
  static final class Field implements Serializable {
    private static final long serialVersionUID = 1L;
    final String name;
    final boolean resourceType;
    final boolean repeating;
    /** The FHIR primitive type code, or null for a complex element. */
    final String prim;
    /** The FHIR type code of the element, such as Reference or Extension. */
    final String fhirType;
    /** The stored Spark type of a primitive, or null for a complex element. */
    final DataType stored;
    Spike08Def child;

    Field(String name, boolean resourceType, boolean repeating, String prim, String fhirType,
        DataType stored) {
      this.name = name;
      this.resourceType = resourceType;
      this.repeating = repeating;
      this.prim = prim;
      this.fhirType = fhirType;
      this.stored = stored;
    }
  }

  final String fhirType;
  final boolean resource;
  final List<Field> fields = new ArrayList<>();
  final Map<String, Field> byName = new HashMap<>();

  private Spike08Def(String fhirType, boolean resource) {
    this.fhirType = fhirType;
    this.resource = resource;
  }

  Field field(String name) {
    return byName.get(name);
  }

  /** Element.id and Extension.url are attributes in XML; the id of a resource is an element. */
  boolean isAttribute(Field f) {
    return ("id".equals(f.name) && !resource) || ("url".equals(f.name) && "Extension".equals(fhirType));
  }

  static Spike08Def forResource(DefinitionContext definitions, String resourceType) {
    return build(DefinitionCanonicalStructure.forResource(definitions, resourceType), resourceType,
        true, new IdentityHashMap<>());
  }

  private static Spike08Def build(DefinitionCanonicalStructure structure, String fhirType,
      boolean resource, IdentityHashMap<DefinitionCanonicalStructure, Spike08Def> memo) {
    final Spike08Def known = memo.get(structure);
    if (known != null) {
      return known;
    }
    final Spike08Def node = new Spike08Def(fhirType, resource);
    memo.put(structure, node);
    for (final LayoutEntry entry : structure.entries()) {
      if (entry.isResourceType()) {
        final Field f = new Field(entry.getName(), true, false, null, null, null);
        node.fields.add(f);
        node.byName.put(f.name, f);
        continue;
      }
      if (!entry.isElement()) {
        continue;
      }
      final String type = entry.getElement().flatMap(e -> e.getFhirType()).map(t -> t.toCode())
          .orElse(null);
      final String prim = entry.primitiveType().map(t -> t.toCode()).orElse(null);
      final DataType stored = entry.primitiveType().flatMap(PrimitiveTypes::storageTypeOf)
          .orElse(null);
      final Field f = new Field(entry.getName(), false, entry.isRepeating(), prim, type, stored);
      node.fields.add(f);
      node.byName.put(f.name, f);
      if (prim == null) {
        structure.elementStructure(entry).ifPresent(child -> f.child = build(child, type, false, memo));
      }
    }
    return node;
  }
}
