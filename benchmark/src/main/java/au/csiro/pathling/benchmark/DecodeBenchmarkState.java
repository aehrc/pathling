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
import au.csiro.pathling.library.io.source.DatasetSource;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.file.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * JMH state class for the decoding benchmark, which measures the cost of turning the Spark
 * representation of a resource back into its JSON representation.
 *
 * <p>The encoded resources are written to a Delta table during setup and read back from it, so that
 * the measured operation starts from persisted columnar data rather than from a lazy encode. The
 * dataset is deliberately not cached, because a decode that read from a cached relation would not
 * be measuring the layout that this programme changes.
 *
 * @author Piotr Szul
 */
@State(Scope.Benchmark)
public class DecodeBenchmarkState {

  /**
   * The main Pathling context that wraps the Spark session and provides the decoder under
   * measurement.
   */
  @Nonnull private final PathlingContext pathlingContext;

  /** The encoded resources, read back from the Delta table written during setup. */
  @Nullable private Dataset<Row> encodedResources;

  /**
   * JMH parameter that selects the resource type to decode. The values match those of {@link
   * EncodeBenchmarkState}, so that the two halves of the round trip can be compared.
   */
  @Nullable
  @Param({"Patient", "Condition", "Encounter", "Observation", "QuestionnaireResponse"})
  private String resourceType;

  /**
   * Setter for the resource type parameter. Used by JMH framework to inject parameter values during
   * benchmark execution.
   *
   * @param resourceType the FHIR resource type to decode
   */
  @SuppressWarnings("unused")
  public void setResourceType(@Nullable final String resourceType) {
    this.resourceType = resourceType;
  }

  /** Constructor initializes the core Spark session and Pathling context. */
  public DecodeBenchmarkState() {
    this.pathlingContext = BenchmarkResources.createPathlingContext("PathlingDecodeBenchmark");
  }

  /**
   * Encodes the NDJSON test data for the selected resource type and writes it to a temporary Delta
   * table, then reads it back as the input to the benchmark.
   */
  @Setup(Level.Trial)
  public void setup() {
    final String type = getResourceType();
    final Dataset<Row> strings = BenchmarkResources.readResourceStrings(pathlingContext, type);
    final DatasetSource datasetSource = pathlingContext.read().datasets();
    datasetSource.dataset(type, pathlingContext.encode(strings, type));

    final Path tempDir = BenchmarkResources.createTempDirectory("pathling-benchmark-decode-");
    datasetSource.write().delta(tempDir.toString());
    this.encodedResources = pathlingContext.read().delta(tempDir.toString()).read(type);
  }

  /**
   * Builds the dataset of decoded resource strings.
   *
   * <p>The result is lazy, and is materialised by the benchmark rather than here, so that the
   * measurement covers the decoding itself rather than the construction of a plan that describes
   * it.
   *
   * @return the dataset of JSON representations of the resources
   */
  @Nonnull
  public Dataset<String> decode() {
    return pathlingContext.decode(
        getEncodedResources(), getResourceType(), PathlingContext.FHIR_JSON);
  }

  /**
   * Provides access to the dataset of encoded resources.
   *
   * @return the dataset of encoded resources
   * @throws IllegalStateException if called before setup() has written and read the resources
   */
  @Nonnull
  public Dataset<Row> getEncodedResources() {
    if (encodedResources == null) {
      throw new IllegalStateException("Encoded resources not read. Ensure setup() is called.");
    }
    return encodedResources;
  }

  /**
   * Provides access to the selected resource type.
   *
   * @return the FHIR resource type being decoded
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
