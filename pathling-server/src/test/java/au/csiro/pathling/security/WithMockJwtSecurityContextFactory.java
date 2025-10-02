package au.csiro.pathling.security;

import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.security.test.context.support.WithSecurityContextFactory;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Felix Naumann
 */
public class WithMockJwtSecurityContextFactory implements WithSecurityContextFactory<WithMockJwt> {

  @Override
  public SecurityContext createSecurityContext(WithMockJwt annotation) {
    SecurityContext context = SecurityContextHolder.createEmptyContext();

    Jwt jwt = Jwt.withTokenValue("mock-token")
        .header("alg", "none")
        .claim("sub", annotation.username())
        .build();

    List<GrantedAuthority> authorities = AuthorityUtils.createAuthorityList(annotation.authorities());

    JwtAuthenticationToken authentication = new JwtAuthenticationToken(jwt, authorities);
    context.setAuthentication(authentication);

    return context;
  }
}
