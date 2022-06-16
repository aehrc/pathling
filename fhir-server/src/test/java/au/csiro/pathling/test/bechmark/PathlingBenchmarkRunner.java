package au.csiro.pathling.test.bechmark;

import lombok.extern.slf4j.Slf4j;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import static au.csiro.pathling.jmh.JmhUtils.buildResultsFileName;

/**
 * Pre-configured bulk benchmark runner.
 */
@Slf4j
public class PathlingBenchmarkRunner {

  public static void main(final String... argc) throws Exception {

    // Optional argument with the path to the output directory
    final String outputDir = (argc.length > 0)
                             ? argc[0]
                             : "target";

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

    log.info("Writing benchmark results to: {}", resultFile);

    Options opt = new OptionsBuilder()
        .include("\\.[^.]+Benchmark\\.[^.]+$")
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
        .jvmArgs("-server")
        .build();
    new Runner(opt).run();
  }

}
