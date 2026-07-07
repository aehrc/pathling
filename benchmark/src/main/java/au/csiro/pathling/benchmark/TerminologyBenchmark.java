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

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Warmup;

/**
 * JMH benchmark comparing local-mode and remote-mode {@code member_of} evaluation.
 *
 * <p>This benchmark measures the wall-clock time to evaluate {@code member_of} against an
 * ECL-defined value set over a large synthetic dataset of SNOMED CT codings, in each of two
 * terminology backends selected by the {@code mode} parameter of {@link TerminologyBenchmarkState}:
 * the executor-side local index service and a remote FHIR terminology server with a warm
 * client-side cache. It exists to characterise success criterion SC-003 - that local mode is at
 * least as fast as remote mode with a warm cache.
 *
 * <p>The benchmark requires a pre-loaded local terminology store and a reference terminology server
 * loaded with the same edition; both are supplied through system properties documented on {@link
 * TerminologyBenchmarkState}. For example:
 *
 * <pre>{@code
 * java -jar benchmark.jar TerminologyBenchmark \
 *   -Dpathling.benchmark.terminology.storagePath=/data/tx-store \
 *   -Dpathling.benchmark.terminology.serverUrl=http://tx.example.org/fhir
 * }</pre>
 *
 * @author John Grimes
 */
@Fork(1)
@Warmup(iterations = 2, time = 10)
@Measurement(iterations = 3, time = 10)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class TerminologyBenchmark {

  /**
   * Evaluates {@code member_of} over the synthetic coding dataset, forcing full execution by
   * counting the members.
   *
   * @param state the benchmark state providing the dataset, the configured Pathling context, and
   *     the value set URL
   * @return the number of codings that are members of the value set
   */
  @Benchmark
  public long memberOf(final TerminologyBenchmarkState state) {
    return state.evaluate();
  }
}
