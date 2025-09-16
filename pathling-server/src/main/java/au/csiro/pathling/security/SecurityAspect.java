/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.security;

import au.csiro.pathling.errors.AccessDeniedError;
import jakarta.annotation.Nonnull;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.jetbrains.annotations.Nullable;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.core.annotation.Order;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.JwtClaimAccessor;
import org.springframework.stereotype.Component;


/**
 * The aspect that inserts checks relating to security.
 *
 * @author Piotr Szul
 * @see <a
 * href="https://docs.spring.io/spring-framework/docs/current/reference/html/core.html#aop-ataspectj-advice-params">Advice
 * Parameters</a>
 */
@Aspect
@Component
@Profile("core")
@ConditionalOnProperty(prefix = "pathling", name = "auth.enabled", havingValue = "true")
@Slf4j
@Order(100)
public class SecurityAspect {

  /**
   * Checks if the current user is authorised to access the resource.
   *
   * @param resourceAccess the resource access required.
   * @param resourceType the resource type.
   * @throws AccessDeniedError if unauthorised.
   */
  @Before("@annotation(resourceAccess) && args(resourceType,..)")
  public void checkResourceRead(@Nonnull final ResourceAccess resourceAccess,
      final ResourceType resourceType) {
    log.debug("Checking access to resource: {}, type: {}", resourceType, resourceAccess.value());
    checkHasAuthority(PathlingAuthority.resourceAccess(resourceAccess.value(), resourceType));
  }

  /**
   * Checks if the current user is authorised to access the operation.
   *
   * @param operationAccess the operation access required.
   * @throws AccessDeniedError if unauthorised.
   */
  @Before("@annotation(operationAccess)")
  public void checkRequiredAuthority(@Nonnull final OperationAccess operationAccess) {
    log.debug("Checking access to operation: {}", operationAccess.value());
    checkHasAuthority(PathlingAuthority.operationAccess(operationAccess.value()));
  }

  /**
   * Checks for the supplied authority and raises an error if it is not present.
   *
   * @param requiredAuthority the authority required for the operation
   */
  public static void checkHasAuthority(@Nonnull final PathlingAuthority requiredAuthority) {
    final Authentication authentication = SecurityContextHolder
        .getContext().getAuthentication();
    final AbstractAuthenticationToken authToken = (authentication instanceof AbstractAuthenticationToken)
                                                  ? (AbstractAuthenticationToken) authentication
                                                  : null;
    if (authToken == null) {
      throw new AccessDeniedError("Token not present");
    }
    final Collection<PathlingAuthority> authorities = authToken.getAuthorities().stream()
        .map(GrantedAuthority::getAuthority)
        .filter(authority -> authority.startsWith("pathling"))
        .map(PathlingAuthority::fromAuthority)
        .collect(Collectors.toList());
    if (authToken.getAuthorities() == null || !requiredAuthority
        .subsumedByAny(authorities)) {
      throw new AccessDeniedError(
          String.format("Missing authority: '%s'", requiredAuthority),
          requiredAuthority.getAuthority());
    }
  }

  @Nonnull
  public static Optional<String> getCurrentUserId(@Nullable final Authentication authentication) {
    String subject = null;
    if (authentication != null) {
      final Object principal = authentication.getPrincipal();
      if (principal instanceof JwtClaimAccessor) {
        subject = ((JwtClaimAccessor) principal).getSubject();
      }
    }
    return Optional.ofNullable(subject);
  }

}
