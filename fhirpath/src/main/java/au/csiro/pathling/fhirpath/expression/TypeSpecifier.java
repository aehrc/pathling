package au.csiro.pathling.fhirpath.expression;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.evaluation.EvaluationContext;
import lombok.Value;
import org.jetbrains.annotations.NotNull;

@Value
public class TypeSpecifier implements FhirPath {

  FhirPathType subjectType;

  /**
   * Creates a new type specifier.
   *
   * @param namespace The namespace
   * @param typeName The type name
   * @throws IllegalArgumentException if the namespace or type name is invalid
   */
  public TypeSpecifier(final String namespace, final String typeName)
      throws IllegalArgumentException {
    this.subjectType = new FhirPathType(namespace, typeName);
  }

  /**
   * Creates a new type specifier with the namespace inferred from the type name.
   *
   * @param typeName The type name
   * @throws IllegalArgumentException if the type name is invalid
   */
  public TypeSpecifier(final String typeName) throws IllegalArgumentException {
    this.subjectType = new FhirPathType(typeName);
  }

  /**
   * Creates a new type specifier with the given {@link FhirPathType}.
   *
   * @param type The {@link FhirPathType}
   */
  public TypeSpecifier(final FhirPathType type) {
    this.subjectType = type;
  }

  @Override
  public @NotNull Collection evaluate(final @NotNull Collection input,
      final @NotNull EvaluationContext context) {
    return null;
  }

  public TypeSpecifier withNamespace(final String namespace) {
    return new TypeSpecifier(subjectType.withNamespace(namespace));
  }

}
