package au.csiro.pathling.security;


import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import javax.annotation.Nonnull;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

public final class SecurityUtils {
  private SecurityUtils() {
  }

  public static void checkHasAuthority(@Nonnull final PathlingAuthority requiredAuthority) {
    AbstractAuthenticationToken auth = (AbstractAuthenticationToken) SecurityContextHolder
        .getContext().getAuthentication();
    if (!auth.getAuthorities().stream().anyMatch(requiredAuthority::matches)) {
      throw BaseServerResponseException
          .newInstance(403, String.format("Requires `%s`", requiredAuthority.toString()));
    }
  }
}
