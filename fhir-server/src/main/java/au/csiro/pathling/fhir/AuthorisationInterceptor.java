/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

import au.csiro.pathling.Configuration.Authorisation;
import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Interceptor;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.rest.api.RestOperationTypeEnum;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.exceptions.*;
import com.auth0.jwk.Jwk;
import com.auth0.jwk.JwkException;
import com.auth0.jwk.JwkProvider;
import com.auth0.jwk.UrlJwkProvider;
import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.AlgorithmMismatchException;
import com.auth0.jwt.exceptions.SignatureVerificationException;
import com.auth0.jwt.exceptions.TokenExpiredException;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.auth0.jwt.interfaces.RSAKeyProvider;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.instance.model.api.IBaseOperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;

/**
 * This class checks each request for authorisation information and rejects the request if it is
 * invalid.
 * <p>
 * This should conform to the SMART App Launch specification, but currently only supporting the
 * scope {@code user/*.read}. If the request does not contain this scope, it will be rejected.
 * <p>
 * The JWT must be signed with the RSA256 algorithm.
 *
 * @author John Grimes
 */
@Interceptor
@Slf4j
public class AuthorisationInterceptor {

  @Nonnull
  private final JWTVerifier verifier;

  /**
   * @param configuration The authorisation section of the {@link au.csiro.pathling.Configuration}.
   * @throws MalformedURLException if there are URLs within the configuration that cannot be parsed
   */
  public AuthorisationInterceptor(@Nonnull final Authorisation configuration)
      throws MalformedURLException {
    final Supplier<RuntimeException> authConfigError = () -> new RuntimeException(
        "Configuration for jwksUrl, issuer and audience must be present if authorisation is enabled");
    final String jwksUrl = configuration.getJwksUrl().orElseThrow(authConfigError);
    final String issuer = configuration.getIssuer().orElseThrow(authConfigError);
    final String audience = configuration.getAudience().orElseThrow(authConfigError);

    final JwkProvider jwkProvider = new UrlJwkProvider(new URL(jwksUrl));
    final RSAKeyProvider keyProvider = new AuthorisationKeyProvider(jwkProvider);
    final Algorithm rsa256 = Algorithm.RSA256(keyProvider);
    verifier = JWT.require(rsa256)
        .withIssuer(issuer)
        .withAudience(audience)
        .build();
  }

  /**
   * HAPI hook to authorise requests before they are processed.
   *
   * @param requestDetails the details of the request
   */
  @Hook(Pointcut.SERVER_INCOMING_REQUEST_PRE_HANDLED)
  @SuppressWarnings("unused")
  public void authoriseRequest(@Nullable final RequestDetails requestDetails) {
    if (requestDetails == null) {
      log.error("Authorisation interceptor invoked with missing request details");
      throw new InternalErrorException("Unexpected error occurred");
    } else {
      // Allow unauthenticated requests to the CapabilityStatement, and also to the
      // OperationDefinitions.
      final boolean metadata = requestDetails.getOperation() != null
          && requestDetails.getOperation().equals("metadata");
      final boolean opDefRead = requestDetails.getResourceName() != null
          && requestDetails.getResourceName().equals("OperationDefinition")
          && requestDetails.getRestOperationType() == RestOperationTypeEnum.READ;
      if (!metadata && !opDefRead) {
        final String token = getBearerToken(requestDetails);
        validateToken(token);
      }
    }
  }

  @Nonnull
  private String getBearerToken(@Nonnull final RequestDetails requestDetails) {
    final String authHeader = requestDetails.getHeader("Authorization");
    if (authHeader == null) {
      throw buildException(Optional.empty(), IssueType.LOGIN,
          AuthenticationException.class);
    }
    if (!authHeader.startsWith("Bearer ")) {
      throw buildException(Optional.of("Authorization header must use Bearer scheme"),
          IssueType.SECURITY, AuthenticationException.class);
    }
    return authHeader.replaceFirst("Bearer ", "");
  }

  private void validateToken(@Nonnull final String token) {
    final DecodedJWT jwt;
    try {
      jwt = verifier.verify(token);
    } catch (final AlgorithmMismatchException e) {
      throw buildException(Optional.of("Signing algorithm must be RSA256"), IssueType.SECURITY,
          AuthenticationException.class);
    } catch (final SignatureVerificationException e) {
      throw buildException(Optional.of("Token signature is invalid"), IssueType.SECURITY,
          AuthenticationException.class);
    } catch (final TokenExpiredException e) {
      throw buildException(Optional.of("Token is expired"), IssueType.SECURITY,
          AuthenticationException.class);
    } catch (final Exception e) {
      throw buildException(Optional.of("Token is invalid"), IssueType.SECURITY,
          AuthenticationException.class);
    }
    final String scope = jwt.getClaim("scope").asString();
    final List<String> scopes = scope == null
                                ? Collections.emptyList()
                                : Arrays.asList(scope.split(" "));
    if (!scopes.contains("user/*.read")) {
      throw buildException(Optional.of("Operation is not authorised by token"), IssueType.FORBIDDEN,
          ForbiddenOperationException.class);
    }
  }

  @Nonnull
  private BaseServerResponseException buildException(@Nonnull final Optional<String> message,
      @Nonnull final IssueType issueType,
      @Nonnull final Class<? extends BaseServerResponseException> exceptionClass) {
    final OperationOutcome opOutcome = new OperationOutcome();
    final OperationOutcomeIssueComponent issue = new OperationOutcomeIssueComponent();
    issue.setSeverity(IssueSeverity.ERROR);
    message.ifPresent(issue::setDiagnostics);
    issue.setCode(issueType);
    opOutcome.addIssue(issue);

    final BaseServerResponseException exception;
    // This is required to get around the fact that HAPI does not allow for the use of an 
    // OperationOutcome resource within the body of a 401 response.
    if (exceptionClass == AuthenticationException.class) {
      exception = new UnclassifiedServerFailureException(401, message.orElse(null), opOutcome);
    } else {
      try {
        final Constructor<? extends BaseServerResponseException> constructor = exceptionClass
            .getConstructor(String.class, IBaseOperationOutcome.class);
        exception = constructor.newInstance(message.orElse(null), opOutcome);
      } catch (final NoSuchMethodException | InstantiationException |
          InvocationTargetException | IllegalAccessException e) {
        return new InternalErrorException("Unexpected error occurred", e);
      }
    }

    return exception;
  }

  private static class AuthorisationKeyProvider implements RSAKeyProvider {

    @Nonnull
    private final JwkProvider jwkProvider;

    private AuthorisationKeyProvider(@Nonnull final JwkProvider jwkProvider) {
      this.jwkProvider = jwkProvider;
    }

    @Override
    @Nullable
    public RSAPublicKey getPublicKeyById(@Nullable final String keyId) {
      final Jwk jwk;
      final RSAPublicKey publicKey;
      if (keyId == null) {
        return null;
      }
      try {
        jwk = jwkProvider.get(keyId);
        publicKey = (RSAPublicKey) jwk.getPublicKey();
      } catch (final JwkException e) {
        return null;
      }
      return publicKey;
    }

    @Override
    @Nullable
    public RSAPrivateKey getPrivateKey() {
      return null;
    }

    @Override
    @Nullable
    public String getPrivateKeyId() {
      return null;
    }

  }

}
