package au.csiro.pathling.security;

import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.security.core.GrantedAuthority;

public class PathlingAuthority {

  private final String authority;

  private PathlingAuthority(String authority) {
    this.authority = authority;
  }

  @Override
  public String toString() {
    return authority;
  }

  public boolean matches(@Nonnull final GrantedAuthority other) {
    return authority.matches(globToRegex(other.getAuthority()));
  }

  public static PathlingAuthority readResource(@Nonnull final ResourceType resourceType) {
    return new PathlingAuthority("user/" + resourceType.toCode().toLowerCase() + ".read");
  }

  public static PathlingAuthority writeResource(@Nonnull final ResourceType resourceType) {
    return new PathlingAuthority("user/" + resourceType.toCode().toLowerCase() + ".write");
  }

  public static PathlingAuthority fromString(@Nonnull final String authority) {
    return new PathlingAuthority(authority);
  }

  private static String globToRegex(@Nonnull final String globExpression) {
    return globExpression.replace(".", "\\.").replace("*", "[\\w]*");
  }

}
