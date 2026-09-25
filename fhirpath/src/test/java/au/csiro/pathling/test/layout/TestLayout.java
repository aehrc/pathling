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
 * The layout dimension of the test framework: which conventions the fields of the test fixtures
 * follow (T037a).
 *
 * <p>The previous layout is the one the existing encoders write, and it is the default. The new
 * layout, Parquet on FHIR, is written by serialising each fixture to FHIR JSON and reading it
 * through the new-layout transform, so the fixtures themselves are unchanged. The layout is chosen
 * with {@code -Dpathling.testLayout=pof} or {@code -Dpathling.testLayout=previous}. Any other value
 * fails rather than falling back to the default, and so does a near miss of the property's name.
 *
 * <p>This is a separate axis from the schema mode ({@link TestSchemaMode}): a schema mode says how
 * much of the layout is present, and a layout says which conventions its fields follow.
 *
 * @author Piotr Szul
 */
public final class TestLayout {

  /** The system property that selects the layout. */
  @Nonnull public static final String PROPERTY = "pathling.testLayout";

  /** The layout the existing encoders write. It is the default until T100g flips it. */
  @Nonnull public static final TestLayout PREVIOUS = new TestLayout("previous");

  /** The Parquet on FHIR layout, read through the new-layout transform. */
  @Nonnull public static final TestLayout POF = new TestLayout("pof");

  @Nonnull private final String name;

  private TestLayout(@Nonnull final String name) {
    this.name = name;
  }

  /**
   * Returns the layout the system properties request, failing loudly where the request cannot be
   * honoured.
   *
   * @return the active layout
   * @throws IllegalArgumentException where the property names no known layout
   * @throws IllegalStateException where a near miss of the property's name is set
   */
  @Nonnull
  public static TestLayout active() {
    return fromProperties(System.getProperties());
  }

  /**
   * Returns the layout a set of properties requests.
   *
   * @param properties the properties to read
   * @return the requested layout
   * @throws IllegalArgumentException where the property names no known layout
   * @throws IllegalStateException where a near miss of the property's name is set
   */
  @Nonnull
  public static TestLayout fromProperties(@Nonnull final Properties properties) {
    return parse(DimensionProperty.read(properties, PROPERTY));
  }

  /**
   * Returns the layout a property value names. An unset property selects the previous layout; any
   * value other than the two known names is refused, including one that differs only in case.
   *
   * @param value the value of the property, or null where it is not set
   * @return the layout
   * @throws IllegalArgumentException where the value names no known layout
   */
  @Nonnull
  public static TestLayout parse(@Nullable final String value) {
    if (value == null || PREVIOUS.name.equals(value)) {
      return PREVIOUS;
    } else if (POF.name.equals(value)) {
      return POF;
    }
    throw new IllegalArgumentException(
        "Unknown test layout '"
            + value
            + "' requested with -D"
            + PROPERTY
            + ". Expected '"
            + PREVIOUS.name
            + "' or '"
            + POF.name
            + "'.");
  }

  /**
   * Returns whether this is the new layout.
   *
   * @return true for the Parquet on FHIR layout
   */
  public boolean isPof() {
    return this == POF;
  }

  /**
   * Returns the name this layout is requested by.
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
