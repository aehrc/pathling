package au.csiro.pathling.fhirpath;

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
    this.namespace = namespace;
    this.typeName = typeName;
  }

  /**
   * Creates a new type specifier with the namespace inferred from the type name.
   *
   * @param typeName The type name
   * @throws IllegalArgumentException if the type name is invalid
   */
  public FhirPathType(final @NotNull String typeName) throws IllegalArgumentException {
    this.namespace = SYSTEM_NAMESPACE;
    this.typeName = typeName;
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

  @Override
  public @NotNull String toString() {
    return namespace + "." + typeName;
  }

  public @NotNull String getTypeName() {
    return typeName;
  }

}
