package au.csiro.pathling.terminology;

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhirpath.encoding.ImmutableCoding;
import au.csiro.pathling.terminology.lookup.LookupExecutor;
import au.csiro.pathling.terminology.lookup.LookupParameters;
import au.csiro.pathling.terminology.subsumes.SubsumesExecutor;
import au.csiro.pathling.terminology.subsumes.SubsumesParameters;
import au.csiro.pathling.terminology.translate.TranslateExecutor;
import au.csiro.pathling.terminology.translate.TranslateParameters;
import au.csiro.pathling.terminology.validatecode.ValidateCodeExecutor;
import au.csiro.pathling.terminology.validatecode.ValidateCodeParameters;
import ca.uhn.fhir.rest.gclient.IOperationUntypedWithInput;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import java.io.Closeable;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome;

/**
 * The default implementation of the TerminologyServiceFactory accessing the REST interface of FHIR
 * compliant terminology server.
 *
 * @author Piotr Szul
 * @author John Grimes
 */
public class DefaultTerminologyService extends BaseTerminologyService {

  public DefaultTerminologyService(@Nonnull final TerminologyClient terminologyClient,
      @Nullable final Closeable toClose) {
    super(terminologyClient, toClose);
  }

  @Override
  public boolean validateCode(@Nonnull final String valueSetUrl, @Nonnull final Coding coding) {
    final ValidateCodeParameters parameters = new ValidateCodeParameters(valueSetUrl,
        ImmutableCoding.of(coding));
    final ValidateCodeExecutor executor = new ValidateCodeExecutor(terminologyClient, parameters);
    return Boolean.TRUE.equals(execute(executor));
  }

  @Nonnull
  @Override
  public List<Translation> translate(@Nonnull final Coding coding,
      @Nonnull final String conceptMapUrl, final boolean reverse, @Nullable final String target) {
    final TranslateParameters parameters = new TranslateParameters(ImmutableCoding.of(coding),
        conceptMapUrl, reverse, target);
    final TranslateExecutor executor = new TranslateExecutor(terminologyClient, parameters);
    return requireNonNull(execute(executor));
  }

  @Nonnull
  @Override
  public ConceptSubsumptionOutcome subsumes(@Nonnull final Coding codingA,
      @Nonnull final Coding codingB) {
    final SubsumesParameters parameters = new SubsumesParameters(
        ImmutableCoding.of(codingA), ImmutableCoding.of(codingB));
    final SubsumesExecutor executor = new SubsumesExecutor(terminologyClient, parameters);
    return requireNonNull(execute(executor));
  }

  @Nonnull
  @Override
  public List<PropertyOrDesignation> lookup(@Nonnull final Coding coding,
      @Nullable final String property) {
    final LookupParameters parameters = new LookupParameters(ImmutableCoding.of(coding), property);
    final LookupExecutor executor = new LookupExecutor(terminologyClient, parameters);
    return requireNonNull(execute(executor));
  }


  @Nullable
  private static <ResponseType, ResultType> ResultType execute(
      @Nonnull final TerminologyOperation<ResponseType, ResultType> operation) {
    final Optional<ResultType> invalidResult = operation.validate();
    if (invalidResult.isPresent()) {
      return invalidResult.get();
    }

    try {
      final IOperationUntypedWithInput<ResponseType> request = operation.buildRequest();
      final ResponseType response = request.execute();
      return operation.extractResult(response);

    } catch (final BaseServerResponseException e) {
      // If the terminology server rejects the request as invalid, use false as the result.
      final ResultType fallback = operation.invalidRequestFallback();
      return handleError(e, fallback);
    }
  }

}
