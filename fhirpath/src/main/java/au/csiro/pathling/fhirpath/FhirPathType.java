package au.csiro.pathling.fhirpath;

import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents one of the types defined within the FHIRPath specification.
 *
 * @author John Grimes
 */
@Getter
public enum FhirPathType {

  BOOLEAN("Boolean"),
  STRING("String"),
  INTEGER("Integer"),
  DECIMAL("Decimal"),
  DATE("Date"),
  DATETIME("DateTime"),
  TIME("Time"),
  QUANTITY("Quantity"),
  CODING("Coding");

  @Nonnull
  private final String typeSpecifier;

  FhirPathType(@Nonnull final String typeSpecifier) {
    this.typeSpecifier = typeSpecifier;
  }

  /**
   * @param fhirType a {@link FHIRDefinedType}
   * @return the corresponding {@link FhirPathType} according to the rules of automatic conversion
   * within the FHIR spec
   */
  @Nonnull
  public static Optional<FhirPathType> forFhirType(@Nonnull final FHIRDefinedType fhirType) {
    return Optional.ofNullable(FhirTypeMapping.get(fhirType));
  }

}