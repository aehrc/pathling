package au.csiro.pathling.fhir;

import au.csiro.pathling.PathlingVersion;
import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Interceptor;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.rest.client.api.IHttpRequest;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Interceptor
public class UserAgentInterceptor {

  public static final String PRODUCT_IDENTIFIER = "pathling";
  @Nonnull
  private final PathlingVersion version;

  public UserAgentInterceptor() {
    this.version = new PathlingVersion();
  }

  @SuppressWarnings("unused")
  @Hook(Pointcut.CLIENT_REQUEST)
  public void handleClientRequest(@Nullable final IHttpRequest httpRequest) {
    if (httpRequest != null) {
      final String userAgent = version.getDescriptiveVersion()
          .map(version -> PRODUCT_IDENTIFIER + "/" + version)
          .orElse(PRODUCT_IDENTIFIER);
      httpRequest.removeHeaders("User-Agent");
      httpRequest.addHeader("User-Agent", userAgent);
    }
  }

}
