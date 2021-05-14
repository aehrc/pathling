package au.csiro.pathling.security;

import java.util.Collection;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.security.core.GrantedAuthority;

/**
 * The representation of the authority supporting wildcard matching and Pathling specfic scopes.
 */
public class PathlingAuthority {

  /**
   * Types of access.
   */
  public enum AccessType {
    READ,
    WRITE;

    public String toCode() {
      return name().toLowerCase();
    }
  }

  public static final String USER_SCOPE = "user";
  public static final String OPERATION_SCOPE = "operation";

  private final String authority;

  private PathlingAuthority(@Nonnull final String authority) {
    validateAuthorityName(authority);
    this.authority = authority;
  }

  /**
   * Getter.
   *
   * @return the name of the authority.
   */
  public String getAuthority() {
    return authority;
  }

  @Override
  public String toString() {
    return authority;
  }

  /**
   * Checks if provided authority encompasses the this one, i.e. is sufficient to access allow
   * access protected by this authority.
   *
   * @param other the authority to check.
   * @return True if provided authority is wider or equal in scope.
   */
  public boolean subsumedBy(@Nonnull final GrantedAuthority other) {
    return authority.matches(globToRegex(other.getAuthority()));
  }

  /**
   * Checks if any of provided authorities subsumes this one.
   *
   * @param others the authorities to check.
   * @return True if any provided authorities is wider or equal in scope.
   */
  public boolean subsumedByAny(@Nonnull final Collection<GrantedAuthority> others) {
    return others.stream().anyMatch(this::subsumedBy);
  }

  /**
   * Constructs the authority required to access the resource.
   *
   * @param resourceType the resource to access.
   * @param accessType the type of access required.
   * @return the authority required for access.
   */
  @Nonnull
  public static PathlingAuthority resourceAccess(@Nonnull final ResourceType resourceType,
      @Nonnull final AccessType accessType) {
    return new PathlingAuthority(
        USER_SCOPE + "/" + resourceType.toCode() + "." + accessType.toCode());

  }

  /**
   * Constructs the authority required to access the operation.
   *
   * @param operationName the name of the operation.
   * @return the authority required for access.
   */
  @Nonnull
  public static PathlingAuthority operationAccess(@Nonnull final String operationName) {
    return new PathlingAuthority(
        OPERATION_SCOPE + ":" + operationName);
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

  private static final Pattern VALID_AUTHORITY_REGEX = Pattern.compile("[\\w.*:/]*");

  /**
   * Allows: word charactes, '.' , ':', '/' and '*'.
   */
  private static void validateAuthorityName(@Nonnull final String authority) {
    if (!VALID_AUTHORITY_REGEX.matcher(authority).matches()) {
      throw new IllegalArgumentException("Illegal characters found in the authority name.");
    }
  }

  private static String globToRegex(@Nonnull final String globExpression) {
    return globExpression.replace(".", "\\.").replace("*", "[\\w]*");
  }
}
