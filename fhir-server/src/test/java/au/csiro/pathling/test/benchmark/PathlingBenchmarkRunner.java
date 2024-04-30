/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.test.benchmark;

import static au.csiro.pathling.jmh.JmhUtils.buildResultsFileName;

import java.io.File;
import java.util.Properties;
import au.csiro.pathling.jmh.SpringBootJmhContext;
import lombok.extern.slf4j.Slf4j;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.springframework.core.io.support.PropertiesLoaderUtils;

/**
 * Pre-configured bulk benchmark runner.
 */
@Slf4j
public class PathlingBenchmarkRunner {

  public static void main(final String... argc) throws Exception {

    // Optional argument with the path to the output directory
    final String outputDir = (argc.length > 0)
                             ? argc[0]
                             : "target/benchmark";

    final Properties properties = PropertiesLoaderUtils.loadAllProperties("benchmark.properties");

    final int warmup = Integer
        .parseInt(properties.getProperty("benchmark.warmup.iterations", "1"));
    final int iterations = Integer
        .parseInt(properties.getProperty("benchmark.test.iterations", "5"));
    final int threads = Integer
        .parseInt(properties.getProperty("benchmark.test.threads", "1"));
    final String resultFilePrefix = properties
        .getProperty("benchmark.global.resultfileprefix", "jmh-");

    final ResultFormatType resultsFileOutputType = ResultFormatType.JSON;
    final String resultFile = buildResultsFileName(outputDir, resultFilePrefix,
        resultsFileOutputType);
    //noinspection ResultOfMethodCallIgnored
    new File(resultFile).getParentFile().mkdirs();

    log.info("Writing benchmark results to: {}", resultFile);

    final Options opt = new OptionsBuilder()
        .include("\\.[^.]+Benchmark\\.[^.]+$")
        .exclude("\\.[^.]+DevBenchmark\\.[^.]+$")
        .warmupIterations(warmup)
        .measurementIterations(iterations)
        // single shot for each iteration:
        .warmupTime(TimeValue.NONE)
        .measurementTime(TimeValue.NONE)
        // do not use forking or the benchmark methods will not see references stored within its class
        .forks(0)
        .threads(threads)
        .shouldDoGC(true)
        .shouldFailOnError(true)
        .resultFormat(resultsFileOutputType)
        .result(resultFile)
        .shouldFailOnError(true)
        .jvmArgs("-server -Xmx4g -ea -Duser.timezone=UTC")
        .build();
    try {
      new Runner(opt).run();
    } finally {
      // clean up the test application contexts created by the benchmarks
      SpringBootJmhContext.cleanUpAll();
    }
  }

}
