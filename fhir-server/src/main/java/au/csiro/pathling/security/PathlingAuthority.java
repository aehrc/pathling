package au.csiro.pathling.security;

import au.csiro.pathling.errors.AccessDeniedError;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

public class PathlingAuthority {

  public enum AccessType {
    READ,
    WRITE;

    public String toCode() {
      return name().toLowerCase();
    }
  }

  private final String authority;

  private PathlingAuthority(String authority) {
    this.authority = authority;
  }

  public String getAuthority() {
    return authority;
  }

  @Override
  public String toString() {
    return authority;
  }

  public boolean matches(@Nonnull final GrantedAuthority other) {
    return authority.matches(globToRegex(other.getAuthority()));
  }

  public void checkAuthorised() {
    AbstractAuthenticationToken auth = (AbstractAuthenticationToken) SecurityContextHolder
        .getContext().getAuthentication();
    if (auth == null || auth.getAuthorities() == null || !auth.getAuthorities().stream()
        .anyMatch(this::matches)) {
      throw new AccessDeniedError(
          String.format("Access denied. Missing authority:`%s`", this.toString()),
          this.getAuthority());
    }
  }

  public static PathlingAuthority resourceAccess(@Nonnull final ResourceType resourceType,
      @Nonnull final AccessType accessType) {
    return new PathlingAuthority(
        "user/" + resourceType.toCode() + "." + accessType.toCode());
  }

  public static PathlingAuthority fromAuthority(@Nonnull final String authority) {
    return new PathlingAuthority(authority);
  }

  private static String globToRegex(@Nonnull final String globExpression) {
    return globExpression.replace(".", "\\.").replace("*", "[\\w]*");
  }
}
