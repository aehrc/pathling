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

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Warmup;

/**
 * JMH benchmarks for Pathling FHIR data processing, measuring each phase of the pipeline
 * separately.
 *
 * <p>An earlier version of these benchmarks reported a single figure for reading NDJSON and running
 * a SQL on FHIR view over it. That figure cannot say whether a change to the storage layout moved
 * the cost of encoding, of decoding, of planning a query or of executing one, because all four are
 * inside it. The four are measured here as:
 *
 * <ul>
 *   <li>{@link #encode} - JSON to the Spark representation.
 *   <li>{@link #decode} - the Spark representation back to JSON.
 *   <li>{@link #viewPlanning} - translating a view definition into a physical Spark plan.
 *   <li>{@link #viewExecution} - running a plan that has already been built.
 * </ul>
 *
 * <p>Spark is lazy, so none of these benchmarks can rely on the construction of a dataset to
 * perform any work. Each one either collects its result or runs it into the {@code noop} data
 * source.
 *
 * @author John Grimes
 */
@Fork(2)
@Warmup(iterations = 1, time = 5)
@Measurement(iterations = 3, time = 5)
@BenchmarkMode({Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class PathlingBenchmark {

  /**
   * Benchmark for encoding resources from their JSON representation into the Spark representation.
   *
   * <p>The encoded dataset is run into the {@code noop} data source, which consumes every column of
   * the encoded schema. See {@link BenchmarkResources#materialise} for why a row count would not do
   * instead.
   *
   * @param state the benchmark state supplying the raw resource strings
   * @return the encoded dataset
   */
  @Benchmark
  public Dataset<Row> encode(@Nonnull final EncodeBenchmarkState state) {
    final Dataset<Row> encoded = state.encode();
    BenchmarkResources.materialise(encoded);
    return encoded;
  }

  /**
   * Benchmark for decoding resources from the Spark representation back into JSON.
   *
   * <p>The input is read from a Delta table written during setup, so that this measures decoding
   * rather than the encode that produced the data.
   *
   * @param state the benchmark state supplying the encoded resources
   * @return the dataset of decoded resource strings
   */
  @Benchmark
  public Dataset<String> decode(@Nonnull final DecodeBenchmarkState state) {
    final Dataset<String> decoded = state.decode();
    BenchmarkResources.materialise(decoded);
    return decoded;
  }

  /**
   * Benchmark for planning a view query, covering the translation of the view definition into
   * FHIRPath columns and the Catalyst analysis, optimisation and physical planning that follow it.
   *
   * <p>No Spark job is submitted, so this measures driver-side work alone.
   *
   * @param state the benchmark state containing the data source and view definition
   * @return the planned dataset
   */
  @Benchmark
  public Dataset<Row> viewPlanning(@Nonnull final PathlingBenchmarkState state) {
    return state.planQuery();
  }

  /**
   * Benchmark for executing a view query whose plan has already been built.
   *
   * <p>The plan is built by an invocation-level setup method which JMH excludes from the
   * measurement, and a new plan is built for every invocation so that no query stage materialised
   * by a previous execution can be reused.
   *
   * @param state the benchmark state holding the prepared query
   * @return the collected rows from executing the view
   */
  @Benchmark
  public List<Row> viewExecution(@Nonnull final PathlingBenchmarkState state) {
    return state.executePreparedQuery();
  }
}
