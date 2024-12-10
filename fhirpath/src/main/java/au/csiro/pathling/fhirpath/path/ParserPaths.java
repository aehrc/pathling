package au.csiro.pathling.fhirpath.path;

import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;
import lombok.Value;
import lombok.experimental.UtilityClass;

/**
 * Helper FhirPath classes for the FHIRPath parser.
 */
@UtilityClass
public class ParserPaths {


  /**
   * Special path used to pass values between visitor in the FHIRPath parser.
   */
  public interface ValuePath<T> extends FhirPath {

    /**
     * @return the value of this path
     */
    T getValue();

    /**
     * {@inheritDoc}
     */
    @Override
    default Collection apply(@Nonnull final Collection input,
        @Nonnull final EvaluationContext context) {
      throw new UnsupportedOperationException("ValuePath cannot be evaluated directly");
    }
  }

  /**
   * FHIRPath expression with a type specifier value.
   */
  @Value
  public static class TypeSpecifierPath implements ValuePath<TypeSpecifier> {

    TypeSpecifier value;
  }

  /**
   * FHIRPath expression with a type namespace value.
   */
  @Value
  public static class TypeNamespacePath implements ValuePath<String> {

    String value;
  }
}
