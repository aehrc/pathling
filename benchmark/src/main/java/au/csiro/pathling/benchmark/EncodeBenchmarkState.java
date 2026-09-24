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

package au.csiro.pathling.benchmark;

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.library.PathlingContext;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * JMH state class for the encoding benchmark, which measures the cost of turning the JSON
 * representation of a resource into the Spark representation of it.
 *
 * <p>This state does not carry the source type parameter of {@link PathlingBenchmarkState}, which
 * selects the storage that a query reads from. Encoding always reads NDJSON text and always
 * produces a dataset, so that parameter would not change what is measured and would only double the
 * number of forks.
 *
 * @author Piotr Szul
 */
@State(Scope.Benchmark)
public class EncodeBenchmarkState {

  /**
   * The main Pathling context that wraps the Spark session and provides the encoder under
   * measurement.
   */
  @Nonnull private final PathlingContext pathlingContext;

  /**
   * The raw JSON representations of the resources, one per row. The read is lazy, so the operation
   * that consumes this dataset pays for reading the file as well as for encoding it.
   */
  @Nullable private Dataset<Row> resourceStrings;

  /**
   * JMH parameter that selects the resource type to encode. The types span a range of schema widths
   * and volumes, which is the dimension that the encoding cost is expected to depend upon.
   */
  @Nullable
  @Param({"Patient", "Condition", "Encounter", "Observation", "QuestionnaireResponse"})
  private String resourceType;

  /**
   * Setter for the resource type parameter. Used by JMH framework to inject parameter values during
   * benchmark execution.
   *
   * @param resourceType the FHIR resource type to encode
   */
  @SuppressWarnings("unused")
  public void setResourceType(@Nullable final String resourceType) {
    this.resourceType = resourceType;
  }

  /** Constructor initializes the core Spark session and Pathling context. */
  public EncodeBenchmarkState() {
    this.pathlingContext = BenchmarkResources.createPathlingContext("PathlingEncodeBenchmark");
  }

  /** Extracts the NDJSON test data for the selected resource type from the classpath. */
  @Setup(Level.Trial)
  public void setup() {
    this.resourceStrings =
        BenchmarkResources.readResourceStrings(pathlingContext, getResourceType());
  }

  /**
   * Builds the dataset of encoded resources.
   *
   * <p>The result is lazy, and is materialised by the benchmark rather than here, so that the
   * measurement covers the encoding itself rather than the construction of a plan that describes
   * it.
   *
   * @return the dataset of encoded resources
   */
  @Nonnull
  public Dataset<Row> encode() {
    return pathlingContext.encode(getResourceStrings(), getResourceType());
  }

  /**
   * Provides access to the dataset of raw resource strings.
   *
   * @return the dataset of raw resource strings
   * @throws IllegalStateException if called before setup() has read the resource strings
   */
  @Nonnull
  public Dataset<Row> getResourceStrings() {
    if (resourceStrings == null) {
      throw new IllegalStateException("Resource strings not read. Ensure setup() is called.");
    }
    return resourceStrings;
  }

  /**
   * Provides access to the selected resource type.
   *
   * @return the FHIR resource type being encoded
   */
  @Nonnull
  public String getResourceType() {
    return requireNonNull(resourceType, "Resource type not set.");
  }

  /** Stops the Spark session to release all associated resources. */
  @TearDown(Level.Trial)
  public void teardown() {
    pathlingContext.getSpark().stop();
  }
}
