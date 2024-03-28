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

package au.csiro.pathling.export.utils;

import au.csiro.pathling.export.fhir.FhirJsonSupport;
import java.net.URI;
import javax.annotation.Nonnull;
import lombok.experimental.UtilityClass;
import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;

/**
 * Utility methods for working with web resources.
 */
@UtilityClass
public class WebUtils {

  /**
   * The content type for FHIR JSON.
   */
  public static final ContentType APPLICATION_FHIR_JSON = ContentType.create(
      "application/fhir+json",
      Consts.UTF_8);
  /**
   * HTTP status code for too many requests.
   */
  public static final int HTTP_TOO_MANY_REQUESTS = 429;


  /**
   * Converts a FHIR resource to an HTTP entity using GSON serialization.
   *
   * @param fhirResource the FHIR resource to convert.
   * @return the HTTP entity.
   */
  @Nonnull
  public static HttpEntity toFhirJsonEntity(@Nonnull final Object fhirResource) {
    return new StringEntity(
        FhirJsonSupport.toJson(fhirResource), APPLICATION_FHIR_JSON);
  }

  /**
   * Ensures that the URI ends with a slash.
   *
   * @param uri the URI to ensure ends with a slash.
   * @return the URI with a trailing slash.
   */
  @Nonnull
  public static URI ensurePathEndsWithSlash(@Nonnull final URI uri) {
    return uri.getPath().endsWith("/")
           ? uri
           : URI.create(uri + "/");
  }
}
