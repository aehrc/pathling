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
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.BenchmarkParams;

/**
 * JMH state class for the encoding benchmarks, which measure the cost of turning the JSON
 * representation of a resource into the Spark representation of it.
 *
 * <p>The resource type to encode is the subject of the benchmark that is running, as given by
 * {@link PathlingBenchmarkResources#subjectOf}.
 *
 * @author Piotr Szul
 * @author John Grimes
 */
@State(Scope.Benchmark)
public class PathlingBenchmarkEncodeState {

  /**
   * The main Pathling context that wraps the Spark session and provides the encoder under
   * measurement.
   */
  @Nonnull private final PathlingContext pathlingContext;

  /** The FHIR resource type to encode. */
  @Nullable private String resourceType;

  /**
   * The raw JSON representations of the resources, one per row. The read is lazy, so the operation
   * that consumes this dataset pays for reading the file as well as for encoding it.
   */
  @Nullable private Dataset<Row> resourceStrings;

  /** Constructor initializes the core Spark session and Pathling context. */
  public PathlingBenchmarkEncodeState() {
    this.pathlingContext =
        PathlingBenchmarkResources.createPathlingContext("PathlingBenchmarkEncode");
  }

  /**
   * Extracts the NDJSON test data for the resource type that the running benchmark names.
   *
   * @param params the parameters of the running benchmark, supplied by JMH
   */
  @Setup(Level.Trial)
  public void setup(@Nonnull final BenchmarkParams params) {
    this.resourceType = PathlingBenchmarkResources.subjectOf(params.getBenchmark());
    this.resourceStrings =
        PathlingBenchmarkResources.readResourceStrings(pathlingContext, resourceType);
  }

  /**
   * Encodes the resources and runs the result into the {@code noop} data source, which consumes
   * every column of the encoded schema. See {@link PathlingBenchmarkResources#materialise} for why
   * a row count would not do instead.
   *
   * @return the encoded dataset
   */
  @Nonnull
  public Dataset<Row> encode() {
    final Dataset<Row> encoded =
        pathlingContext.encode(
            requireNonNull(resourceStrings, "Resource strings not read. Ensure setup() is called."),
            requireNonNull(resourceType));
    PathlingBenchmarkResources.materialise(encoded);
    return encoded;
  }

  /** Stops the Spark session to release all associated resources. */
  @TearDown(Level.Trial)
  public void teardown() {
    pathlingContext.getSpark().stop();
  }
}
