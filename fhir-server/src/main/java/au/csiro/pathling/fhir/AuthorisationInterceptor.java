/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

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
 * scope "user/*.read". If the request does not contain this scope, it will be rejected.
 * <p>
 * The JWT must be signed with the RSA256 algorithm.
 *
 * @author John Grimes
 */
@Interceptor
public class AuthorisationInterceptor {

  private final JWTVerifier verifier;

  public AuthorisationInterceptor(String jwksUrl, String issuer, String audience)
      throws MalformedURLException {
    JwkProvider jwkProvider = new UrlJwkProvider(new URL(jwksUrl));
    AuthorisationKeyProvider keyProvider = new AuthorisationKeyProvider(jwkProvider);
    Algorithm rsa256 = Algorithm.RSA256(keyProvider);
    verifier = JWT.require(rsa256)
        .withIssuer(issuer)
        .withAudience(audience)
        .build();
  }

  @Hook(Pointcut.SERVER_INCOMING_REQUEST_PRE_HANDLED)
  public void authoriseRequest(RequestDetails requestDetails) {
    // Allow unauthenticated requests to the CapabilityStatement, and also to the
    // OperationDefinitions.
    boolean metadata = requestDetails.getOperation() != null
        && requestDetails.getOperation().equals("metadata");
    boolean opDefRead = requestDetails.getResourceName() != null
        && requestDetails.getResourceName().equals("OperationDefinition")
        && requestDetails.getRestOperationType() == RestOperationTypeEnum.READ;
    if (!metadata && !opDefRead) {
      String token = getBearerToken(requestDetails);
      validateToken(token);
    }
  }

  private String getBearerToken(RequestDetails requestDetails) {
    String authHeader = requestDetails.getHeader("Authorization");
    if (authHeader == null) {
      throw buildException(null, IssueType.LOGIN,
          AuthenticationException.class);
    }
    if (!authHeader.startsWith("Bearer ")) {
      throw buildException("Authorization header must use Bearer scheme", IssueType.SECURITY,
          AuthenticationException.class);
    }
    return authHeader.replaceFirst("Bearer ", "");
  }

  private void validateToken(String token) {
    DecodedJWT jwt;
    try {
      jwt = verifier.verify(token);
    } catch (AlgorithmMismatchException e) {
      throw buildException("Signing algorithm must be RSA256", IssueType.SECURITY,
          AuthenticationException.class);
    } catch (SignatureVerificationException e) {
      throw buildException("Token signature is invalid", IssueType.SECURITY,
          AuthenticationException.class);
    } catch (TokenExpiredException e) {
      throw buildException("Token is expired", IssueType.SECURITY, AuthenticationException.class);
    } catch (Exception e) {
      throw buildException("Token is invalid", IssueType.SECURITY, AuthenticationException.class);
    }
    String scope = jwt.getClaim("scope").asString();
    List<String> scopes = scope == null
                          ? Collections.emptyList()
                          : Arrays.asList(scope.split(" "));
    if (!scopes.contains("user/*.read")) {
      throw buildException("Operation is not authorised by token", IssueType.FORBIDDEN,
          ForbiddenOperationException.class);
    }
  }

  private BaseServerResponseException buildException(String message, IssueType issueType,
      Class<? extends BaseServerResponseException> exceptionClass) {
    OperationOutcome opOutcome = new OperationOutcome();
    OperationOutcomeIssueComponent issue = new OperationOutcomeIssueComponent();
    issue.setSeverity(IssueSeverity.ERROR);
    if (message != null) {
      issue.setDiagnostics(message);
    }
    issue.setCode(issueType);
    opOutcome.addIssue(issue);

    BaseServerResponseException exception;
    // This is required to get around the fact that HAPI does not allow for the use of an OperationOutcome resource within the body of a 401 response.
    if (exceptionClass == AuthenticationException.class) {
      exception = new UnclassifiedServerFailureException(401, message, opOutcome);
    } else {
      try {
        Constructor<? extends BaseServerResponseException> constructor = exceptionClass
            .getConstructor(String.class, IBaseOperationOutcome.class);
        exception = constructor.newInstance(message, opOutcome);
      } catch (NoSuchMethodException | InstantiationException | InvocationTargetException | IllegalAccessException e) {
        return new InternalErrorException("Unexpected error occurred", e);
      }
    }

    return exception;
  }

  private static class AuthorisationKeyProvider implements RSAKeyProvider {

    private final JwkProvider jwkProvider;

    public AuthorisationKeyProvider(JwkProvider jwkProvider) {
      this.jwkProvider = jwkProvider;
    }

    @Override
    public RSAPublicKey getPublicKeyById(String keyId) {
      Jwk jwk;
      RSAPublicKey publicKey;
      try {
        jwk = jwkProvider.get(keyId);
        publicKey = (RSAPublicKey) jwk.getPublicKey();
      } catch (JwkException e) {
        return null;
      }
      return publicKey;
    }

    @Override
    public RSAPrivateKey getPrivateKey() {
      return null;
    }

    @Override
    public String getPrivateKeyId() {
      return null;
    }

  }

}
