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

import au.csiro.pathling.security.ResourceAccess.AccessType;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.Value;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * The representation of the authority supporting wildcard matching and Pathling-specific scopes.
 *
 * @author Piotr Szul
 */
@Getter
public class PathlingAuthority {

  private static final Pattern VALID_AUTHORITY_REGEX =
      Pattern.compile("pathling(?::([a-zA-Z-]+))?(?::([a-zA-Z]+))?");
  private static final String WILDCARD = "*";

  private final String authority;
  private final Optional<String> action;
  private final Optional<String> subject;

  private PathlingAuthority(@Nonnull final String authority) {
    final Permission permission = validateAuthorityName(authority);
    this.authority = authority;
    action = permission.getAction();
    subject = permission.getSubject();
  }

  @Override
  public String toString() {
    return authority;
  }

  /**
   * Checks if provided authority encompasses this one, i.e. is sufficient to allow the access
   * provided by this authority.
   *
   * @param other the authority to check.
   * @return True if provided authority is wider or equal in scope.
   */
  public boolean subsumedBy(@Nonnull final PathlingAuthority other) {
    if (other.getAction().isEmpty() && other.getSubject().isEmpty()) {
      return true;
    } else if (other.getAction().equals(action) && other.getSubject().isEmpty()) {
      return true;
    } else {
      return other.getAction().equals(action) && other.getSubject().equals(subject);
    }
  }

  /**
   * Checks if any of provided authorities subsumes this one.
   *
   * @param others the authorities to check.
   * @return True if any provided authorities is wider or equal in scope.
   */
  public boolean subsumedByAny(@Nonnull final Collection<PathlingAuthority> others) {
    return others.stream().anyMatch(this::subsumedBy);
  }

  /**
   * Constructs the authority required to access the resource.
   *
   * @param accessType the type of access required.
   * @param resourceType the resource to access.
   * @return the authority required for access.
   */
  @Nonnull
  public static PathlingAuthority resourceAccess(@Nonnull final AccessType accessType,
      @Nonnull final ResourceType resourceType) {
    return new PathlingAuthority("pathling:" + accessType.getCode() + ":" + resourceType.toCode());

  }

  /**
   * Constructs the authority required to access the operation.
   *
   * @param operationName the name of the operation.
   * @return the authority required for access.
   */
  @Nonnull
  public static PathlingAuthority operationAccess(@Nonnull final String operationName) {
    return new PathlingAuthority("pathling:" + operationName);
  }

  /**
   * Constructs an arbitrary authority.
   *
   * @param authorityName the name of the authority.
   * @return the authority.
   */
  @Nonnull
  public static PathlingAuthority fromAuthority(@Nonnull final String authorityName) {
    return new PathlingAuthority(authorityName);
  }

  /**
   * Allows: word characters, ':' and '*'.
   */
  @Nonnull
  private static Permission validateAuthorityName(@Nonnull final CharSequence authority) {
    final Matcher matcher = VALID_AUTHORITY_REGEX.matcher(authority);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Authority is not recognized: " + authority);
    } else {
      final Optional<String> action = Optional.ofNullable(matcher.group(1));
      final Optional<String> subject = Optional.ofNullable(matcher.group(2));

      final List<String> accessCodes = Arrays.stream(ResourceAccess.AccessType.values())
          .map(ResourceAccess.AccessType::getCode)
          .toList();
      if (action.isPresent() && !accessCodes.contains(action.get()) && subject.isPresent()) {
        throw new IllegalArgumentException("Subject not supported for action: " + action);
      }
      return new Permission(action, subject);
    }
  }

  @Value
  private static class Permission {

    Optional<String> action;
    Optional<String> subject;

  }

}
