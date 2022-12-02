package au.csiro.pathling.terminology;

import static au.csiro.pathling.fhir.ParametersUtils.partsToBean;
import static au.csiro.pathling.fhir.ParametersUtils.toBooleanResult;
import static au.csiro.pathling.fhir.ParametersUtils.toSubsumptionOutcome;
import static au.csiro.pathling.fhir.ParametersUtils.toMatchParts;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static java.util.function.Predicate.not;
import static org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome.NOTSUBSUMED;

import au.csiro.pathling.fhir.ParametersUtils;
import au.csiro.pathling.fhir.ParametersUtils.PropertyPart;
import au.csiro.pathling.fhir.TerminologyClient2;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.Type;
import org.hl7.fhir.r4.model.UriType;
import org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome;

/**
 * The default implementation of the TerminologyServiceFactory2 accessing the REST interface of FHIR
 * compliant terminology server.
 */
public class DefaultTerminologyService2 implements TerminologyService2, Closeable {

  @Nonnull
  private final TerminologyClient2 terminologyClient;

  @Nullable
  private final Closeable toClose;

  public DefaultTerminologyService2(@Nonnull final TerminologyClient2 terminologyClient,
      @Nullable Closeable toClose) {
    this.terminologyClient = terminologyClient;
    this.toClose = toClose;
  }

  @Nullable
  private static <T> T optional(@Nonnull final Function<String, T> converter,
      @Nullable String value) {
    return value != null
           ? converter.apply(value)
           : null;
  }

  @Nonnull
  private static <T> T required(@Nonnull final Function<String, T> converter,
      @Nullable String value) {
    return converter.apply(Objects.requireNonNull(value));
  }

  @Nonnull
  private static List<Translation> toTranslations(final @Nonnull Parameters parameters) {
    return toMatchParts(parameters)
        .map(tp -> Translation.of(ConceptMapEquivalence.fromCode(tp.getEquivalence().getCode()),
            tp.getConcept()))
        .collect(Collectors.toUnmodifiableList());
  }


  @Override
  public boolean validateCode(@Nonnull final String codeSystemUrl, @Nonnull final Coding coding) {

    if (isNull(coding.getSystem()) || isNull(coding.getCode())) {
      return false;
    }

    return toBooleanResult(terminologyClient.validateCode(
        required(UriType::new, codeSystemUrl), required(UriType::new, coding.getSystem()),
        optional(StringType::new, coding.getVersion()),
        required(CodeType::new, coding.getCode())
    ));
  }

  @Nonnull
  @Override
  public List<Translation> translate(@Nonnull final Coding coding,
      @Nonnull final String conceptMapUrl,
      final boolean reverse,
      @Nullable final String target) {

    if (isNull(coding.getSystem()) || isNull(coding.getCode())) {
      return Collections.emptyList();
    }

    return toTranslations(terminologyClient.translate(
        required(UriType::new, conceptMapUrl),
        required(UriType::new, coding.getSystem()),
        optional(StringType::new, coding.getVersion()),
        required(CodeType::new, coding.getCode()),
        new BooleanType(reverse),
        optional(UriType::new, target)
    ));
  }

  @Nonnull
  @Override
  public ConceptSubsumptionOutcome subsumes(@Nonnull final Coding codingA,
      @Nonnull final Coding codingB) {

    if (codingA.getSystem() == null || !codingA.getSystem().equals(codingB.getSystem())) {
      return NOTSUBSUMED;
    }

    if (codingA.getCode() == null || codingA.getCode() == null) {
      return NOTSUBSUMED;
    }

    final String resolvedSystem = codingA.getSystem();
    // TODO: Check how that should work with versions (e.g. how should we treat the case of null version with non null version)
    // if both version are present then ten need to be equal
    if (!(codingA.getVersion() == null || codingB.getVersion() == null || codingA.getVersion()
        .equals(codingB.getVersion()))) {
      return NOTSUBSUMED;
    }
    final String resolvedVersion = codingA.getVersion() != null
                                   ? codingA.getVersion()
                                   : codingB.getVersion();

    // TODO: optimize not call the client if not needed (when codings are equal)
    return toSubsumptionOutcome(terminologyClient.subsumes(
        required(CodeType::new, codingA.getCode()),
        required(CodeType::new, codingB.getCode()),
        required(UriType::new, resolvedSystem),
        optional(StringType::new, resolvedVersion)
    ));
  }


  @Nonnull
  private static List<PropertyOrDesignation> toPropertiesAndDesignations(
      @Nonnull final Parameters parameters,
      @Nullable final String propertyCode) {

    final List<PropertyPart> x = ParametersUtils.toPropertiesAndDesignations(
        parameters).collect(Collectors.toUnmodifiableList());
    return x.stream()
        .flatMap(part -> nonNull(part.getSubproperty())
                         ? part.getSubproperty().stream()
                         : Stream.of(part))
        .map(part -> Property.of(part.getCode().getValue(), requireNonNull(part.getValue())))
        .filter(property -> Objects.isNull(propertyCode) || propertyCode.equals(property.getCode()))
        .collect(Collectors.toUnmodifiableList());
  }

  @Nonnull
  @Override
  public List<PropertyOrDesignation> lookup(@Nonnull final Coding coding,
      @Nullable final String property,
      @Nullable final String displayLanguage) {

    if (isNull(coding.getSystem()) || isNull(coding.getCode())) {
      return Collections.emptyList();
    }

    return toPropertiesAndDesignations(terminologyClient.lookup(
        required(UriType::new, coding.getSystem()),
        optional(StringType::new, coding.getVersion()),
        required(CodeType::new, coding.getCode()),
        optional(CodeType::new, property),
        optional(CodeType::new, displayLanguage)
    ), property);
  }

  @Override
  public void close() throws IOException {
    if (nonNull(toClose)) {
      toClose.close();
    }
  }
}
