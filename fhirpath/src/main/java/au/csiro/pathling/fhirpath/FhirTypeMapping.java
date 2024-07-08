package au.csiro.pathling.fhirpath;

import com.google.common.collect.ImmutableMap;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Map;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Maps FHIR types to FHIRPath types.
 *
 * @author John Grimes
 * @see <a href="https://hl7.org/fhir/R4/fhirpath.html#types">Using FHIR types in expressions</a>
 */
public abstract class FhirTypeMapping {

  @Nonnull
  private static final Map<FHIRDefinedType, FhirPathType> FHIR_TO_FHIRPATH_TYPE =
      new ImmutableMap.Builder<FHIRDefinedType, FhirPathType>()
          .put(FHIRDefinedType.BOOLEAN, FhirPathType.BOOLEAN)
          .put(FHIRDefinedType.STRING, FhirPathType.STRING)
          .put(FHIRDefinedType.URI, FhirPathType.STRING)
          .put(FHIRDefinedType.CODE, FhirPathType.STRING)
          .put(FHIRDefinedType.OID, FhirPathType.STRING)
          .put(FHIRDefinedType.ID, FhirPathType.STRING)
          .put(FHIRDefinedType.UUID, FhirPathType.STRING)
          .put(FHIRDefinedType.MARKDOWN, FhirPathType.STRING)
          .put(FHIRDefinedType.BASE64BINARY, FhirPathType.STRING)
          .put(FHIRDefinedType.INTEGER, FhirPathType.INTEGER)
          .put(FHIRDefinedType.UNSIGNEDINT, FhirPathType.INTEGER)
          .put(FHIRDefinedType.POSITIVEINT, FhirPathType.INTEGER)
          .put(FHIRDefinedType.DECIMAL, FhirPathType.DECIMAL)
          .put(FHIRDefinedType.DATE, FhirPathType.DATETIME)
          .put(FHIRDefinedType.DATETIME, FhirPathType.DATETIME)
          .put(FHIRDefinedType.INSTANT, FhirPathType.DATETIME)
          .put(FHIRDefinedType.TIME, FhirPathType.TIME)
          .put(FHIRDefinedType.QUANTITY, FhirPathType.QUANTITY)
          .put(FHIRDefinedType.CODING, FhirPathType.CODING)
          .build();

  /**
   * @return the FHIRPath type that the given FHIR type can be automatically converted to
   */
  @Nullable
  public static FhirPathType get(@Nonnull final FHIRDefinedType fhirType) {
    return FhirTypeMapping.FHIR_TO_FHIRPATH_TYPE.get(fhirType);
  }

}
