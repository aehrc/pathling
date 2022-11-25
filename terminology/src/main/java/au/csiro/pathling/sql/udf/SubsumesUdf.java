package au.csiro.pathling.sql.udf;

import static au.csiro.pathling.sql.udf.TerminologyUdfHelpers.decodeOneOrMany;
import static au.csiro.pathling.sql.udf.TerminologyUdfHelpers.validCodings;
import static org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome.EQUIVALENT;
import static org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome.SUBSUMEDBY;
import static org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome.SUBSUMES;

import au.csiro.pathling.terminology.TerminologyService2;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome;

@Slf4j
public class SubsumesUdf implements SqlFunction,
    SqlFunction3<Object, Object, Boolean, Boolean> {

  private static final long serialVersionUID = 7605853352299165569L;

  public static final String FUNCTION_NAME = "subsumes";

  public static final DataType RETURN_TYPE = DataTypes.BooleanType;
  public static boolean PARAM_INVERTED_DEFAULT = false;

  @Nonnull
  private final TerminologyServiceFactory terminologyServiceFactory;

  public SubsumesUdf(@Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
    this.terminologyServiceFactory = terminologyServiceFactory;
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public DataType getReturnType() {
    return RETURN_TYPE;
  }

  @Nullable
  protected Boolean doCall(@Nullable final Stream<Coding> codingsA,
      @Nullable final Stream<Coding> codingsB,
      @Nullable Boolean inverted) {
    if (codingsA == null || codingsB == null) {
      return null;
    }
    final boolean resolvedInverted = inverted != null
                                     ? inverted
                                     : PARAM_INVERTED_DEFAULT;

    final TerminologyService2 terminologyService = terminologyServiceFactory.buildService2();

    // does any of the input codings subsume any of the output codings (within the same system)
    final List<Coding> validCodingsB = validCodings(codingsB)
        .collect(Collectors.toUnmodifiableList());

    return validCodings(codingsA)
        .anyMatch(codingA ->
            validCodingsB.stream()
                .filter(codingB -> codingA.getSystem().equals(codingB.getSystem()))
                .anyMatch(codingB -> isSubsumes(terminologyService.subsumes(codingA, codingB),
                    resolvedInverted))
        );
  }

  @Nullable
  @Override
  public Boolean call(@Nullable final Object codingRowOrArrayA,
      @Nullable final Object codingRowOrArrayB,
      @Nullable final Boolean inverted) {
    return doCall(
        decodeOneOrMany(codingRowOrArrayA),
        decodeOneOrMany(codingRowOrArrayB, 1),
        inverted);
  }

  private static boolean isSubsumes(@Nonnull final ConceptSubsumptionOutcome outcome,
      final boolean inverted) {
    return EQUIVALENT.equals(outcome) || (inverted
                                          ? SUBSUMEDBY
                                          : SUBSUMES).equals(outcome);
  }
}
