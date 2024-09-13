package au.csiro.pathling.fhirpath;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.jetbrains.annotations.NotNull;

/**
 * Represents one of the types defined within the FHIRPath specification.
 *
 * @author John Grimes
 */
public class FhirPathType {

  public static final FhirPathType BOOLEAN;
  public static final FhirPathType STRING;
  public static final FhirPathType INTEGER;
  public static final FhirPathType DECIMAL;
  public static final FhirPathType DATE;
  public static final FhirPathType DATE_TIME;
  public static final FhirPathType TIME;
  public static final FhirPathType QUANTITY;
  public static final FhirPathType ANY;
  public static final FhirPathType CODING;
  private static final String SYSTEM_NAMESPACE = "System";
  private static final String FHIR_NAMESPACE = "FHIR";
  private static final List<String> NAMESPACE_SEARCH_ORDER = List.of(FHIR_NAMESPACE,
      SYSTEM_NAMESPACE);
  private static final Set<String> FHIRPATH_TYPES = Set.of("Boolean", "String", "Integer",
      "Decimal", "Date", "DateTime", "Time", "Quantity", "Any");
  private static final Map<String, Predicate<String>> NAMESPACE_VALIDATORS = Map.of(
      FHIR_NAMESPACE, FhirPathType::isValidFhirType,
      SYSTEM_NAMESPACE, FhirPathType::isValidSystemType
  );

  static {
    BOOLEAN = new FhirPathType("Boolean");
    STRING = new FhirPathType("String");
    INTEGER = new FhirPathType("Integer");
    DECIMAL = new FhirPathType("Decimal");
    DATE = new FhirPathType("Date");
    DATE_TIME = new FhirPathType("DateTime");
    TIME = new FhirPathType("Time");
    QUANTITY = new FhirPathType("Quantity");
    ANY = new FhirPathType("Any");
    CODING = new FhirPathType("FHIR", "Coding");
  }

  private final @NotNull String namespace;
  private final @NotNull String typeName;

  /**
   * Creates a new type specifier.
   *
   * @param namespace The namespace
   * @param typeName The type name
   * @throws IllegalArgumentException if the namespace or type name is invalid
   */
  public FhirPathType(final @NotNull String namespace, final @NotNull String typeName)
      throws IllegalArgumentException {
    this.namespace = validateNamespace(namespace);
    this.typeName = validateTypeName(typeName, namespace);
  }

  /**
   * Creates a new type specifier with the namespace inferred from the type name.
   *
   * @param typeName The type name
   * @throws IllegalArgumentException if the type name is invalid
   */
  public FhirPathType(final @NotNull String typeName) throws IllegalArgumentException {
    this.namespace = searchForTypeName(typeName);
    this.typeName = typeName;
  }

  private static @NotNull String validateNamespace(final @NotNull String namespace)
      throws IllegalArgumentException {
    if (!NAMESPACE_VALIDATORS.containsKey(namespace)) {
      throw new IllegalArgumentException("Invalid namespace: " + namespace);
    }
    return namespace;
  }

  private static @NotNull String validateTypeName(final @NotNull String typeName,
      final @NotNull String namespace)
      throws IllegalArgumentException {
    if (!NAMESPACE_VALIDATORS.get(namespace).test(typeName)) {
      throw new IllegalArgumentException("Invalid type name: " + typeName);
    }
    return typeName;
  }

  private static @NotNull String searchForTypeName(final @NotNull String typeName) {
    for (final String namespace : NAMESPACE_SEARCH_ORDER) {
      if (NAMESPACE_VALIDATORS.get(namespace).test(typeName)) {
        return namespace;
      }
    }
    throw new IllegalArgumentException("Invalid type name: " + typeName);
  }

  private static boolean isValidFhirType(final @NotNull String typeName) {
    try {
      return FHIRDefinedType.fromCode(typeName) != null;
    } catch (final FHIRException e) {
      return false;
    }
  }

  private static boolean isValidSystemType(final @NotNull String typeName) {
    return FHIRPATH_TYPES.contains(typeName);
  }

  /**
   * Returns a copy of this type specifier with the new namespace.
   *
   * @param namespace the new namespace
   * @return the type specifier with different namespace
   */
  public @NotNull FhirPathType withNamespace(final @NotNull String namespace) {
    return new FhirPathType(namespace, typeName);
  }

  /**
   * Converts this type specifier to a FHIR type.
   *
   * @return the FHIR type
   * @throws IllegalStateException if this type specifier is not a FHIR type
   */
  public @NotNull FHIRDefinedType toFhirType() {
    if (!isFhirType()) {
      throw new IllegalStateException("Not a FHIR type: " + this);
    }
    return FHIRDefinedType.fromCode(typeName);
  }

  /**
   * @return true if this type specifier is a type in FHIR namespace.
   */
  private boolean isFhirType() {
    return namespace.equals(FHIR_NAMESPACE);
  }

  /**
   * @return The FHIR resource type represented by this type specifier
   */
  public @NotNull ResourceType toResourceType() {
    if (!isFhirType()) {
      throw new IllegalStateException("Not a FHIR type: " + this);
    }
    return ResourceType.fromCode(typeName);
  }

  @Override
  public @NotNull String toString() {
    return namespace + "." + typeName;
  }

  public @NotNull String getNamespace() {
    return namespace;
  }

  public @NotNull String getTypeName() {
    return typeName;
  }

}
