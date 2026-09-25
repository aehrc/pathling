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
package au.csiro.pathling.test.layout;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Locale;
import java.util.Properties;

/**
 * Reads the system property that selects one test dimension, and refuses a property that looks like
 * it was meant to select it but is spelled differently.
 *
 * <p>This repository has test configuration that silently does nothing when it is misnamed, so a
 * near miss such as {@code pathling.test.layout} or {@code pathling.testlayout} fails rather than
 * leaving the dimension on its default while the build reports green.
 *
 * @author Piotr Szul
 */
final class DimensionProperty {

  private DimensionProperty() {}

  /**
   * Returns the value of a dimension's property, failing if any other property is a near miss of
   * its name.
   *
   * @param properties the properties to read, normally the system properties
   * @param name the exact name of the property
   * @return the value, or null where the property is not set
   * @throws IllegalStateException where a near miss of the name is set
   */
  @Nullable
  static String read(@Nonnull final Properties properties, @Nonnull final String name) {
    final String key = normalised(name);
    properties.stringPropertyNames().stream()
        .filter(candidate -> !candidate.equals(name))
        .filter(candidate -> normalised(candidate).equals(key))
        .findFirst()
        .ifPresent(
            candidate -> {
              throw new IllegalStateException(
                  "The system property '"
                      + candidate
                      + "' is set, which is not read. Did you mean '"
                      + name
                      + "'?");
            });
    return properties.getProperty(name);
  }

  @Nonnull
  private static String normalised(@Nonnull final String name) {
    return name.replaceAll("[._-]", "").toLowerCase(Locale.ROOT);
  }
}
