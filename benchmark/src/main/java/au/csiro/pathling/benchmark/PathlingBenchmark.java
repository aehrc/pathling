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
 * a SQL on FHIR view over it. That figure cannot say whether a change moved the cost of encoding,
 * of decoding, of planning a query or of executing one, because all four are inside it. The four
 * are measured here separately, by benchmarks whose names begin with {@code encode}, {@code
 * decode}, {@code plan} and {@code execute}:
 *
 * <ul>
 *   <li>{@code encode} - JSON to the Spark representation, for one resource type.
 *   <li>{@code decode} - the Spark representation back to JSON, for one resource type.
 *   <li>{@code plan} - translating one view definition into a physical Spark plan.
 *   <li>{@code execute} - running the plan of one view definition that has already been built.
 * </ul>
 *
 * <p>The rest of each name is the subject of the benchmark: the resource type or the view
 * definition. The benchmarks take no JMH parameters, so that each result is identified by its name
 * alone. This keeps results comparable when the benchmarks are run against older releases, and
 * allows a subject to be selected or excluded with a regular expression, as in {@code -e
 * QuestionnaireResponseFlat}. The state classes work out what to measure from the name of the
 * running benchmark, using {@link PathlingBenchmarkResources#subjectOf}.
 *
 * <p>Spark is lazy, so none of these benchmarks can rely on the construction of a dataset to
 * perform any work. Each one either collects its result or runs it into the {@code noop} data
 * source, except for the planning benchmarks, which force the physical plan without running it.
 *
 * <p>These benchmarks are also built against earlier releases, so they must only use API that is
 * present in every release that is benchmarked.
 *
 * @author John Grimes
 */
@Fork(2)
@Warmup(iterations = 1, time = 5)
@Measurement(iterations = 3, time = 5)
@BenchmarkMode({Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class PathlingBenchmark {

  // Encoding: JSON to the Spark representation.

  /**
   * Encodes the Patient test data.
   *
   * @param state the benchmark state
   * @return the result, returned so that JMH consumes it
   */
  @Benchmark
  public Dataset<Row> encodePatient(@Nonnull final PathlingBenchmarkEncodeState state) {
    return state.encode();
  }

  /**
   * Encodes the Condition test data.
   *
   * @param state the benchmark state
   * @return the result, returned so that JMH consumes it
   */
  @Benchmark
  public Dataset<Row> encodeCondition(@Nonnull final PathlingBenchmarkEncodeState state) {
    return state.encode();
  }

  /**
   * Encodes the Encounter test data.
   *
   * @param state the benchmark state
   * @return the result, returned so that JMH consumes it
   */
  @Benchmark
  public Dataset<Row> encodeEncounter(@Nonnull final PathlingBenchmarkEncodeState state) {
    return state.encode();
  }

  /**
   * Encodes the Observation test data.
   *
   * @param state the benchmark state
   * @return the result, returned so that JMH consumes it
   */
  @Benchmark
  public Dataset<Row> encodeObservation(@Nonnull final PathlingBenchmarkEncodeState state) {
    return state.encode();
  }

  /**
   * Encodes the QuestionnaireResponse test data.
   *
   * @param state the benchmark state
   * @return the result, returned so that JMH consumes it
   */
  @Benchmark
  public Dataset<Row> encodeQuestionnaireResponse(
      @Nonnull final PathlingBenchmarkEncodeState state) {
    return state.encode();
  }

  // Decoding: the Spark representation back to JSON.

  /**
   * Decodes the Patient test data.
   *
   * @param state the benchmark state
   * @return the result, returned so that JMH consumes it
   */
  @Benchmark
  public Dataset<String> decodePatient(@Nonnull final PathlingBenchmarkDecodeState state) {
    return state.decode();
  }

  /**
   * Decodes the Condition test data.
   *
   * @param state the benchmark state
   * @return the result, returned so that JMH consumes it
   */
  @Benchmark
  public Dataset<String> decodeCondition(@Nonnull final PathlingBenchmarkDecodeState state) {
    return state.decode();
  }

  /**
   * Decodes the Encounter test data.
   *
   * @param state the benchmark state
   * @return the result, returned so that JMH consumes it
   */
  @Benchmark
  public Dataset<String> decodeEncounter(@Nonnull final PathlingBenchmarkDecodeState state) {
    return state.decode();
  }

  /**
   * Decodes the Observation test data.
   *
   * @param state the benchmark state
   * @return the result, returned so that JMH consumes it
   */
  @Benchmark
  public Dataset<String> decodeObservation(@Nonnull final PathlingBenchmarkDecodeState state) {
    return state.decode();
  }

  /**
   * Decodes the QuestionnaireResponse test data.
   *
   * @param state the benchmark state
   * @return the result, returned so that JMH consumes it
   */
  @Benchmark
  public Dataset<String> decodeQuestionnaireResponse(
      @Nonnull final PathlingBenchmarkDecodeState state) {
    return state.decode();
  }

  // Planning: translating a view definition into a physical Spark plan.

  /**
   * Plans the ConditionFlat view.
   *
   * @param state the benchmark state
   * @return the result, returned so that JMH consumes it
   */
  @Benchmark
  public Dataset<Row> planConditionFlat(@Nonnull final PathlingBenchmarkViewState state) {
    return state.planQuery();
  }

  /**
   * Plans the EncounterFlat view.
   *
   * @param state the benchmark state
   * @return the result, returned so that JMH consumes it
   */
  @Benchmark
  public Dataset<Row> planEncounterFlat(@Nonnull final PathlingBenchmarkViewState state) {
    return state.planQuery();
  }

  /**
   * Plans the PatientAddresses view.
   *
   * @param state the benchmark state
   * @return the result, returned so that JMH consumes it
   */
  @Benchmark
  public Dataset<Row> planPatientAddresses(@Nonnull final PathlingBenchmarkViewState state) {
    return state.planQuery();
  }

  /**
   * Plans the PatientAndContactAddressUnion view.
   *
   * @param state the benchmark state
   * @return the result, returned so that JMH consumes it
   */
  @Benchmark
  public Dataset<Row> planPatientAndContactAddressUnion(
      @Nonnull final PathlingBenchmarkViewState state) {
    return state.planQuery();
  }

  /**
   * Plans the PatientDemographics view.
   *
   * @param state the benchmark state
   * @return the result, returned so that JMH consumes it
   */
  @Benchmark
  public Dataset<Row> planPatientDemographics(@Nonnull final PathlingBenchmarkViewState state) {
    return state.planQuery();
  }

  /**
   * Plans the UsCoreBloodPressures view.
   *
   * @param state the benchmark state
   * @return the result, returned so that JMH consumes it
   */
  @Benchmark
  public Dataset<Row> planUsCoreBloodPressures(@Nonnull final PathlingBenchmarkViewState state) {
    return state.planQuery();
  }

  /**
   * Plans the QuestionnaireResponseFlat view.
   *
   * @param state the benchmark state
   * @return the result, returned so that JMH consumes it
   */
  @Benchmark
  public Dataset<Row> planQuestionnaireResponseFlat(
      @Nonnull final PathlingBenchmarkViewState state) {
    return state.planQuery();
  }

  // Execution: running a view plan that has already been built.

  /**
   * Executes the ConditionFlat view.
   *
   * @param state the benchmark state
   * @return the result, returned so that JMH consumes it
   */
  @Benchmark
  public List<Row> executeConditionFlat(@Nonnull final PathlingBenchmarkViewState state) {
    return state.executePreparedQuery();
  }

  /**
   * Executes the EncounterFlat view.
   *
   * @param state the benchmark state
   * @return the result, returned so that JMH consumes it
   */
  @Benchmark
  public List<Row> executeEncounterFlat(@Nonnull final PathlingBenchmarkViewState state) {
    return state.executePreparedQuery();
  }

  /**
   * Executes the PatientAddresses view.
   *
   * @param state the benchmark state
   * @return the result, returned so that JMH consumes it
   */
  @Benchmark
  public List<Row> executePatientAddresses(@Nonnull final PathlingBenchmarkViewState state) {
    return state.executePreparedQuery();
  }

  /**
   * Executes the PatientAndContactAddressUnion view.
   *
   * @param state the benchmark state
   * @return the result, returned so that JMH consumes it
   */
  @Benchmark
  public List<Row> executePatientAndContactAddressUnion(
      @Nonnull final PathlingBenchmarkViewState state) {
    return state.executePreparedQuery();
  }

  /**
   * Executes the PatientDemographics view.
   *
   * @param state the benchmark state
   * @return the result, returned so that JMH consumes it
   */
  @Benchmark
  public List<Row> executePatientDemographics(@Nonnull final PathlingBenchmarkViewState state) {
    return state.executePreparedQuery();
  }

  /**
   * Executes the UsCoreBloodPressures view.
   *
   * @param state the benchmark state
   * @return the result, returned so that JMH consumes it
   */
  @Benchmark
  public List<Row> executeUsCoreBloodPressures(@Nonnull final PathlingBenchmarkViewState state) {
    return state.executePreparedQuery();
  }

  /**
   * Executes the QuestionnaireResponseFlat view.
   *
   * @param state the benchmark state
   * @return the result, returned so that JMH consumes it
   */
  @Benchmark
  public List<Row> executeQuestionnaireResponseFlat(
      @Nonnull final PathlingBenchmarkViewState state) {
    return state.executePreparedQuery();
  }
}
