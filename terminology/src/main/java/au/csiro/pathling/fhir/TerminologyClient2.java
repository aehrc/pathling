package au.csiro.pathling.fhir;

import au.csiro.pathling.config.TerminologyAuthConfiguration;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.api.IRestfulClientFactory;
import ca.uhn.fhir.rest.client.api.ServerValidationModeEnum;
import ca.uhn.fhir.rest.client.interceptor.LoggingInterceptor;
import org.apache.http.client.HttpClient;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CodeSystem;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.UriType;
import org.hl7.fhir.r4.model.ValueSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

public interface TerminologyClient2 {

  Logger log = LoggerFactory.getLogger(TerminologyClient2.class);

  @Operation(name = "$validate-code", type = ValueSet.class, idempotent = true)
  @Nonnull
  Parameters validateCode(
      @Nonnull @OperationParam(name = "url") UriType url,
      @Nonnull @OperationParam(name = "system") UriType system,
      @Nullable @OperationParam(name = "systemVersion") StringType version,
      @Nonnull @OperationParam(name = "code") CodeType code
  );


  @Operation(name = "$translate", type = CodeSystem.class, idempotent = true)
  @Nonnull
  Parameters translate(
      @Nonnull @OperationParam(name = "url") UriType url,
      @Nonnull @OperationParam(name = "system") UriType system,
      @Nullable @OperationParam(name = "version") StringType version,
      @Nonnull @OperationParam(name = "code") CodeType code,
      @Nullable @OperationParam(name = "reverse") BooleanType reverse
  );

  @Operation(name = "$subsumes", type = CodeSystem.class, idempotent = true)
  @Nonnull
  Parameters subsumes(
      @Nonnull @OperationParam(name = "codeA") CodeType codeA,
      @Nonnull @OperationParam(name = "codeB") CodeType codeB,
      @Nonnull @OperationParam(name = "system") UriType system,
      @Nullable @OperationParam(name = "version") StringType version
  );


  static TerminologyClient2 build(@Nonnull final FhirContext fhirContext,
      @Nonnull final String terminologyServerUrl,
      final boolean verboseRequestLogging, @Nonnull final TerminologyAuthConfiguration authConfig,
      @Nonnull final
      HttpClient httpClient) {
    final IRestfulClientFactory restfulClientFactory = fhirContext.getRestfulClientFactory();
    restfulClientFactory.setHttpClient(httpClient);
    restfulClientFactory.setServerValidationMode(ServerValidationModeEnum.NEVER);

    final IGenericClient genericClient = restfulClientFactory.newGenericClient(
        terminologyServerUrl);

    genericClient.registerInterceptor(new UserAgentInterceptor());

    final LoggingInterceptor loggingInterceptor = new LoggingInterceptor();
    loggingInterceptor.setLogger(log);
    loggingInterceptor.setLogRequestSummary(true);
    loggingInterceptor.setLogResponseSummary(true);
    loggingInterceptor.setLogRequestHeaders(false);
    loggingInterceptor.setLogResponseHeaders(false);
    if (verboseRequestLogging) {
      loggingInterceptor.setLogRequestBody(true);
      loggingInterceptor.setLogResponseBody(true);
    }
    genericClient.registerInterceptor(loggingInterceptor);

    if (authConfig.isEnabled()) {
      checkNotNull(authConfig.getTokenEndpoint());
      checkNotNull(authConfig.getClientId());
      checkNotNull(authConfig.getClientSecret());
      final ClientAuthInterceptor clientAuthInterceptor = new ClientAuthInterceptor(
          authConfig.getTokenEndpoint(), authConfig.getClientId(), authConfig.getClientSecret(),
          authConfig.getScope(), authConfig.getTokenExpiryTolerance());
      genericClient.registerInterceptor(clientAuthInterceptor);
    }

    return new TerminologyClient2Impl(genericClient);
  }
}

class TerminologyClient2Impl implements TerminologyClient2 {

  @Nonnull
  final IGenericClient fhirClient;

  TerminologyClient2Impl(@Nonnull final IGenericClient fhirClient) {
    this.fhirClient = fhirClient;
  }

  @Nonnull
  @Override
  public Parameters validateCode(@Nonnull final UriType url, @Nonnull final UriType system,
      @Nullable final StringType version, @Nonnull final CodeType code) {
    final Parameters params = new Parameters();
    params.addParameter().setName("url").setValue(url);
    params.addParameter().setName("system").setValue(system);
    params.addParameter().setName("code").setValue(code);
    if (version != null) {
      params.addParameter().setName("systemVersion").setValue(version);
    }

    return fhirClient.operation()
        .onType(ValueSet.class)
        .named("$validate-code")
        .withParameters(params)
        .useHttpGet()
        .execute();
  }

  @Nonnull
  @Override
  public Parameters translate(@Nonnull final UriType url, @Nonnull final UriType system,
      @Nullable final StringType version, @Nonnull final CodeType code,
      @Nullable final BooleanType reverse) {
    final Parameters params = new Parameters();
    params.addParameter().setName("url").setValue(url);
    params.addParameter().setName("system").setValue(system);
    params.addParameter().setName("code").setValue(code);
    if (version != null) {
      params.addParameter().setName("version").setValue(version);
    }
    if (reverse != null) {
      params.addParameter().setName("reverse").setValue(reverse);
    }
    return fhirClient.operation()
        .onType(ConceptMap.class)
        .named("$translate")
        .withParameters(params)
        .useHttpGet()
        .execute();
  }

  @Nonnull
  @Override
  public Parameters subsumes(@Nonnull final CodeType codeA, @Nonnull final CodeType codeB,
      @Nonnull final UriType system, @Nullable final StringType version) {
    final Parameters params = new Parameters();
    params.addParameter().setName("codeA").setValue(codeA);
    params.addParameter().setName("codeB").setValue(codeB);
    params.addParameter().setName("system").setValue(system);
    if (version != null) {
      params.addParameter().setName("version").setValue(version);
    }
    return fhirClient.operation()
        .onType(CodeSystem.class)
        .named("$subsumes")
        .withParameters(params)
        .useHttpGet()
        .execute();
  }
}
