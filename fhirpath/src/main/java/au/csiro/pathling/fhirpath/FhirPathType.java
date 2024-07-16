package au.csiro.pathling.fhirpath;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import lombok.Getter;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Represents one of the types defined within the FHIRPath specification.
 *
 * @author John Grimes
 */
@Getter
public class FhirPathType {

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

  public static final FhirPathType BOOLEAN = new FhirPathType("Boolean");
  public static final FhirPathType STRING = new FhirPathType("String");
  public static final FhirPathType INTEGER = new FhirPathType("Integer");
  public static final FhirPathType DECIMAL = new FhirPathType("Decimal");
  public static final FhirPathType DATE = new FhirPathType("Date");
  public static final FhirPathType DATE_TIME = new FhirPathType("DateTime");
  public static final FhirPathType TIME = new FhirPathType("Time");
  public static final FhirPathType QUANTITY = new FhirPathType("Quantity");
  public static final FhirPathType ANY = new FhirPathType("Any");
  public static final FhirPathType CODING = new FhirPathType("FHIR", "Coding");

  final String namespace;
  final String typeName;

  /**
   * Creates a new type specifier.
   *
   * @param namespace The namespace
   * @param typeName The type name
   * @throws IllegalArgumentException if the namespace or type name is invalid
   */
  public FhirPathType(final String namespace, final String typeName)
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
  public FhirPathType(final String typeName) throws IllegalArgumentException {
    this.namespace = searchForTypeName(typeName);
    this.typeName = typeName;
  }

  /**
   * @return true if this type specifier is a type in FHIR namespace.
   */
  public boolean isFhirType() {
    return namespace.equals(FHIR_NAMESPACE);
  }


  /**
   * Returns a copy of this type specifier with the new namespace.
   *
   * @param namespace the new namespace
   * @return the type specifier with different namespace
   */
  public FhirPathType withNamespace(final String namespace) {
    return new FhirPathType(namespace, typeName);
  }

  /**
   * Converts this type specifier to a FHIR type.
   *
   * @return the FHIR type
   * @throws IllegalStateException if this type specifier is not a FHIR type
   */
  public FHIRDefinedType toFhirType() {
    if (!isFhirType()) {
      throw new IllegalStateException("Not a FHIR type: " + this);
    }
    return FHIRDefinedType.fromCode(typeName);
  }

  /**
   * @return The FHIR resource type represented by this type specifier
   */
  public ResourceType toResourceType() {
    if (!isFhirType()) {
      throw new IllegalStateException("Not a FHIR type: " + this);
    }
    return ResourceType.fromCode(typeName);
  }

  private static String validateNamespace(final String namespace) throws IllegalArgumentException {
    if (!NAMESPACE_VALIDATORS.containsKey(namespace)) {
      throw new IllegalArgumentException("Invalid namespace: " + namespace);
    }
    return namespace;
  }

  private static String validateTypeName(final String typeName, final String namespace)
      throws IllegalArgumentException {
    if (!NAMESPACE_VALIDATORS.get(namespace).test(typeName)) {
      throw new IllegalArgumentException("Invalid type name: " + typeName);
    }
    return typeName;
  }

  private static String searchForTypeName(final String typeName) {
    for (final String namespace : NAMESPACE_SEARCH_ORDER) {
      if (NAMESPACE_VALIDATORS.get(namespace).test(typeName)) {
        return namespace;
      }
    }
    throw new IllegalArgumentException("Invalid type name: " + typeName);
  }

  private static boolean isValidFhirType(final String typeName) {
    try {
      return FHIRDefinedType.fromCode(typeName) != null;
    } catch (final FHIRException e) {
      return false;
    }
  }

  private static boolean isValidSystemType(final String typeName) {
    return FHIRPATH_TYPES.contains(typeName);
  }

  @Override
  public String toString() {
    return namespace + "." + typeName;
  }
 
}
