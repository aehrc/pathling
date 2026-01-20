/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.operations.bulkimport;

import au.csiro.pathling.cache.CacheableDatabase;
import au.csiro.pathling.config.ImportConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.errors.SecurityError;
import jakarta.annotation.Nonnull;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

/**
 * This class encapsulates security rules for accessing external resources.
 *
 * @author Piotr Szul
 */
@Component
@Slf4j
public class AccessRules {

  @Nonnull private final List<String> allowableSources;

  /**
   * Creates a new AccessRules instance.
   *
   * @param configuration a {@link ServerConfiguration} object which controls the behaviour of the
   *     AccessRules
   */
  public AccessRules(@Nonnull final ServerConfiguration configuration) {

    final ImportConfiguration importConfiguration = configuration.getImport();
    if (importConfiguration == null) {
      this.allowableSources = List.of();
    } else {
      final List<String> sources = importConfiguration.getAllowableSources();
      this.allowableSources =
          sources.stream()
              .filter(StringUtils::isNotBlank)
              .map(CacheableDatabase::convertS3ToS3aUrl)
              .toList();
      if (allowableSources.size() < sources.size()) {
        log.warn(
            "Some empty or blank allowable sources have been ignored in import configuration.");
      }
    }

    if (allowableSources.isEmpty()) {
      log.warn("There are NO allowable sources defined in the configuration for import.");
    }
  }

  /**
   * Checks if data import is allowed from given URL.
   *
   * @param url the URL to check
   * @throws SecurityError if the URL is not an allowed import source
   */
  public void checkCanImportFrom(@Nonnull final String url) {
    if (!canImportFrom(url)) {
      throw new AccessDeniedError("URL: '" + url + "' is not an allowed source for import.");
    }
  }

  private boolean canImportFrom(@Nonnull final String url) {
    return allowableSources.stream().anyMatch(url::startsWith);
  }
}
