package au.csiro.pathling.fhirpath;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Maps FHIR types to FHIRPath types.
 *
 * @author John Grimes
 * @see <a href="https://hl7.org/fhir/R4/fhirpath.html#types">Using FHIR types in expressions</a>
 */
public abstract class FhirTypeMapping {

  @NotNull
  private static final Map<FHIRDefinedType, FhirPathType> FHIR_TO_FHIRPATH_TYPE =
      new ImmutableMap.Builder<FHIRDefinedType, FhirPathType>()
          .put(FHIRDefinedType.BOOLEAN, new FhirPathType("Boolean"))
          .put(FHIRDefinedType.STRING, new FhirPathType("String"))
          .put(FHIRDefinedType.URI, new FhirPathType("String"))
          .put(FHIRDefinedType.CODE, new FhirPathType("String"))
          .put(FHIRDefinedType.OID, new FhirPathType("String"))
          .put(FHIRDefinedType.ID, new FhirPathType("String"))
          .put(FHIRDefinedType.UUID, new FhirPathType("String"))
          .put(FHIRDefinedType.MARKDOWN, new FhirPathType("String"))
          .put(FHIRDefinedType.BASE64BINARY, new FhirPathType("String"))
          .put(FHIRDefinedType.INTEGER, new FhirPathType("Integer"))
          .put(FHIRDefinedType.UNSIGNEDINT, new FhirPathType("Integer"))
          .put(FHIRDefinedType.POSITIVEINT, new FhirPathType("Integer"))
          .put(FHIRDefinedType.DECIMAL, new FhirPathType("Decimal"))
          .put(FHIRDefinedType.DATE, new FhirPathType("DateTime"))
          .put(FHIRDefinedType.DATETIME, new FhirPathType("DateTime"))
          .put(FHIRDefinedType.INSTANT, new FhirPathType("DateTime"))
          .put(FHIRDefinedType.TIME, new FhirPathType("Time"))
          .put(FHIRDefinedType.QUANTITY, new FhirPathType("Quantity"))
          .build();

  /**
   * @return the FHIRPath type that the given FHIR type can be automatically converted to
   */
  @NotNull
  public static Optional<FhirPathType> get(@Nullable final FHIRDefinedType fhirType) {
    return Optional.ofNullable(FhirTypeMapping.FHIR_TO_FHIRPATH_TYPE.get(fhirType));
  }

}
