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

package au.csiro.pathling.config;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.resource.PathResourceResolver;

/**
 * Web MVC configuration for serving the admin UI as a single-page application.
 *
 * @author John Grimes
 */
@Configuration
public class WebConfiguration implements WebMvcConfigurer {

  private static final String[] ADMIN_UI_PATHS = {"/admin/", "/admin/**"};
  private static final String ADMIN_UI_RESOURCE_LOCATION = "classpath:/static/admin/";
  private static final String INDEX_HTML = "static/admin/index.html";

  @Override
  public void addViewControllers(@Nonnull final ViewControllerRegistry registry) {
    // Redirect root to admin UI.
    registry.addRedirectViewController("/", "/admin/");
    // Redirect /admin to /admin/ for consistent URL handling.
    registry.addRedirectViewController("/admin", "/admin/");
  }

  @Override
  public void addResourceHandlers(@Nonnull final ResourceHandlerRegistry registry) {
    // Serve admin UI static resources with SPA fallback to index.html.
    registry.addResourceHandler(ADMIN_UI_PATHS)
        .addResourceLocations(ADMIN_UI_RESOURCE_LOCATION)
        .resourceChain(true)
        .addResolver(new PathResourceResolver() {
          @Override
          protected Resource getResource(@Nonnull final String resourcePath,
              @Nonnull final Resource location) throws IOException {
            // Serve index.html for empty path (root of /admin/).
            if (resourcePath.isEmpty()) {
              return new ClassPathResource(INDEX_HTML);
            }
            final Resource requested = location.createRelative(resourcePath);
            // If the requested resource exists, serve it; otherwise serve index.html for
            // client-side routing.
            return requested.exists() && requested.isReadable()
                   ? requested
                   : new ClassPathResource(INDEX_HTML);
          }
        });
  }

}
