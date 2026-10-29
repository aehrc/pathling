import au.csiro.pathling.definition.DefinitionContext;
import au.csiro.pathling.definition.ElementDefinition;
import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.schema.DefinitionCanonicalStructure;
import au.csiro.pathling.schema.LayoutEntry;
import au.csiro.pathling.schema.LayoutFields;
import ca.uhn.fhir.context.FhirContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Counts the leaf columns of a dense schema walked from the canonical structure, bounded the way the
 * previous encoder's options bound it, and how many of them annotations would add.
 *
 * <p>Usage: DenseAnnotationWidth <resource> <maxNesting> <extensions:true|false>
 */
public class DenseAnnotationWidth {

  static final Set<String> OPEN_TYPES =
      Set.of(
          "boolean", "code", "date", "dateTime", "decimal", "integer", "string", "Coding",
          "CodeableConcept", "Address", "Identifier", "Reference");

  static int maxNesting;
  static boolean extensions;
  static final Map<String, Integer> counts = new TreeMap<>();

  public static void main(final String[] args) {
    final DefinitionContext definitions = FhirDefinitionContext.of(FhirContext.forR4());
    maxNesting = Integer.parseInt(args[1]);
    extensions = Boolean.parseBoolean(args[2]);
    walk(DefinitionCanonicalStructure.forResource(definitions, args[0]), new ArrayList<>(), false);
    System.out.println(args[0] + " maxNesting=" + maxNesting + " extensions=" + extensions);
    counts.forEach((k, v) -> System.out.println("  " + k + " = " + v));
  }

  static void walk(
      final DefinitionCanonicalStructure node, final List<String> path, final boolean inExtension) {
    for (final LayoutEntry entry : node.entries()) {
      if (entry.isResourceType()) {
        inc("leaf.element");
        continue;
      }
      final ElementDefinition element = entry.getElement().orElseThrow();
      final String type = element.getFhirType().orElseThrow().toCode();
      if (inExtension && entry.getName().startsWith("value") && !OPEN_TYPES.contains(type)) {
        continue;
      }
      if (!extensions
          && (entry.getName().equals("extension") || entry.getName().equals("modifierExtension"))) {
        continue;
      }
      if (entry.isMetadataGroup()) {
        inc("leaf.metadataGroup(not counted in total)");
        continue;
      }
      if (entry.isAnnotation()) {
        final String kind =
            entry.getName().endsWith(LayoutFields.NUMERIC_SUFFIX)
                ? "numeric"
                : entry.getName().endsWith(LayoutFields.CANONICAL_EXACT_SUFFIX)
                    ? "canonical_exact"
                    : entry.getName().endsWith(LayoutFields.CANONICAL_SUFFIX)
                        ? "canonical"
                        : "range";
        inc("annotation." + kind);
        continue;
      }
      if (entry.primitiveType().isPresent()) {
        inc("leaf.element");
        continue;
      }
      final long seen = path.stream().filter(type::equals).count();
      if (seen > maxNesting) {
        continue;
      }
      node.elementStructure(entry)
          .ifPresent(
              child -> {
                path.add(type);
                walk(child, path, type.equals("Extension"));
                path.remove(path.size() - 1);
              });
    }
  }

  static void inc(final String key) {
    counts.merge(key, 1, Integer::sum);
  }
}
