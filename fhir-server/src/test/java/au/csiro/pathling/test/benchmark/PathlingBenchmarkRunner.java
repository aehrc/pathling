package au.csiro.pathling.test.benchmark;

import static au.csiro.pathling.jmh.JmhUtils.buildResultsFileName;

import java.io.File;
import java.util.Properties;
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
        .jvmArgs("-server -Xmx6g -ea -Duser.timezone=UTC")
        .build();
    new Runner(opt).run();
  }

}
