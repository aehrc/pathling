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
import java.util.Properties;

/**
 * The schema mode dimension of the test framework: how much of the layout a fixture's schema
 * carries (T037).
 *
 * <p>Pruned is the only mode. The dense mode that Parquet on FHIR planned is superseded by the
 * defined-schema loads of the YAML suites (specification 2881, R23), so a request for any other
 * value fails rather than being answered with a pruned schema, and so does a near miss of the
 * property's name.
 *
 * <p>The mode describes the new-layout arm of the fixtures. The previous layout carries the schema
 * its encoders write, whatever the mode.
 *
 * @author Piotr Szul
 */
public final class TestSchemaMode {

  /** The system property that selects the schema mode. */
  @Nonnull public static final String PROPERTY = "pathling.testSchemaMode";

  /** A schema carrying only what the data carries, and the default. */
  @Nonnull public static final TestSchemaMode PRUNED = new TestSchemaMode("pruned");

  @Nonnull private final String name;

  private TestSchemaMode(@Nonnull final String name) {
    this.name = name;
  }

  /**
   * Returns the schema mode the system properties request, failing loudly where the request cannot
   * be honoured.
   *
   * @return the active schema mode
   * @throws IllegalArgumentException where the property names no available mode
   * @throws IllegalStateException where a near miss of the property's name is set
   */
  @Nonnull
  public static TestSchemaMode active() {
    return fromProperties(System.getProperties());
  }

  /**
   * Returns the schema mode a set of properties requests.
   *
   * @param properties the properties to read
   * @return the requested schema mode
   * @throws IllegalArgumentException where the property names no available mode
   * @throws IllegalStateException where a near miss of the property's name is set
   */
  @Nonnull
  public static TestSchemaMode fromProperties(@Nonnull final Properties properties) {
    return parse(DimensionProperty.read(properties, PROPERTY));
  }

  /**
   * Returns the schema mode a property value names. An unset property selects the pruned mode.
   *
   * @param value the value of the property, or null where it is not set
   * @return the schema mode
   * @throws IllegalArgumentException where the value names no available mode
   */
  @Nonnull
  public static TestSchemaMode parse(@Nullable final String value) {
    if (value == null || PRUNED.name.equals(value)) {
      return PRUNED;
    }
    throw new IllegalArgumentException(
        "Unknown schema mode '"
            + value
            + "' requested with -D"
            + PROPERTY
            + ". Only '"
            + PRUNED.name
            + "' is available.");
  }

  /**
   * Returns the name this mode is requested by.
   *
   * @return the name
   */
  @Nonnull
  public String getName() {
    return name;
  }

  @Override
  @Nonnull
  public String toString() {
    return name;
  }
}
