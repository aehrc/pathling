/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.io;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.errors.SecurityError;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * This class encapsulates security rules for accessing external resources.
 *
 * @author Piotr Szul
 */
@Component
@Profile("core")
@Slf4j
public class AccessRules {

  @Nonnull
  private final List<String> allowableSources;

  /**
   * @param configuration a {@link Configuration} object which controls the behaviour of the
   * AccessRules
   */
  public AccessRules(@Nonnull final Configuration configuration) {

    this.allowableSources = configuration.getImport().getAllowableSources().stream()
        .filter(StringUtils::isNotBlank)
        .map(PersistenceScheme::convertS3ToS3aUrl).collect(
            Collectors.toList());

    if (allowableSources.size() < configuration.getImport().getAllowableSources().size()) {
      log.warn("Some empty or blank allowable sources have been ignored in import configuration.");
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
      throw new SecurityError("URL: '" + url + "' is not an allowed source for import.");
    }
  }

  private boolean canImportFrom(@Nonnull final String url) {
    return allowableSources.stream().anyMatch(url::startsWith);
  }

}
