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
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.BenchmarkParams;

/**
 * JMH state class for the decoding benchmarks, which measure the cost of turning the Spark
 * representation of a resource back into JSON.
 *
 * <p>The resource type to decode is the subject of the benchmark that is running, as given by
 * {@link PathlingBenchmarkResources#subjectOf}. The encoded resources are written to a Delta table
 * during setup and read back from it, so that the measured operation starts from persisted columnar
 * data rather than from a lazy encode. The dataset is deliberately not cached, because a decode
 * that read from a cached relation would not be measuring the storage layout.
 *
 * @author Piotr Szul
 * @author John Grimes
 */
@State(Scope.Benchmark)
public class PathlingBenchmarkDecodeState {

  /**
   * The main Pathling context that wraps the Spark session and provides the decoder under
   * measurement.
   */
  @Nonnull private final PathlingContext pathlingContext;

  /** The FHIR resource type to decode. */
  @Nullable private String resourceType;

  /** The encoded resources, read back from the Delta table written during setup. */
  @Nullable private Dataset<Row> encodedResources;

  /** Constructor initializes the core Spark session and Pathling context. */
  public PathlingBenchmarkDecodeState() {
    this.pathlingContext =
        PathlingBenchmarkResources.createPathlingContext("PathlingBenchmarkDecode");
  }

  /**
   * Encodes the NDJSON test data for the resource type that the running benchmark names and writes
   * it to a temporary Delta table, then reads it back as the input to the benchmark.
   *
   * @param params the parameters of the running benchmark, supplied by JMH
   */
  @Setup(Level.Trial)
  public void setup(@Nonnull final BenchmarkParams params) {
    final String type = PathlingBenchmarkResources.subjectOf(params.getBenchmark());
    final Dataset<Row> strings =
        PathlingBenchmarkResources.readResourceStrings(pathlingContext, type);
    final DatasetSource datasetSource = pathlingContext.read().datasets();
    datasetSource.dataset(type, pathlingContext.encode(strings, type));

    final Path tempDir =
        PathlingBenchmarkResources.createTempDirectory("pathling-benchmark-decode-");
    datasetSource.write().delta(tempDir.toString());
    this.resourceType = type;
    this.encodedResources = pathlingContext.read().delta(tempDir.toString()).read(type);
  }

  /**
   * Decodes the resources and runs the result into the {@code noop} data source, so that the
   * measurement covers the decoding itself rather than the construction of a plan that describes
   * it.
   *
   * @return the dataset of JSON representations of the resources
   */
  @Nonnull
  public Dataset<String> decode() {
    final Dataset<String> decoded =
        pathlingContext.decode(
            requireNonNull(
                encodedResources, "Encoded resources not read. Ensure setup() is called."),
            requireNonNull(resourceType),
            PathlingContext.FHIR_JSON);
    PathlingBenchmarkResources.materialise(decoded);
    return decoded;
  }

  /** Stops the Spark session to release all associated resources. */
  @TearDown(Level.Trial)
  public void teardown() {
    pathlingContext.getSpark().stop();
  }
}
