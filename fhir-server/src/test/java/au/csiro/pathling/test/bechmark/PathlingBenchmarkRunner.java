package au.csiro.pathling.test.bechmark;

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

public class PathlingBenchmarkRunner {

  public static void main(String... argc) throws Exception {

    final String outputDir = (argc.length > 0)
                             ? argc[0]
                             : "target";

    Properties properties = PropertiesLoaderUtils.loadAllProperties("benchmark.properties");

    int warmup = Integer.parseInt(properties.getProperty("benchmark.warmup.iterations", "1"));
    int iterations = Integer.parseInt(properties.getProperty("benchmark.test.iterations", "3"));
    int threads = Integer.parseInt(properties.getProperty("benchmark.test.threads", "1"));
    String resultFilePrefix = properties.getProperty("benchmark.global.resultfileprefix", "jmh-");

    ResultFormatType resultsFileOutputType = ResultFormatType.JSON;

    Options opt = new OptionsBuilder()
        .include("\\." + "[^.]+Benchmark" + "\\.")
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
        .result(buildResultsFileName(outputDir, resultFilePrefix, resultsFileOutputType))
        .shouldFailOnError(true)
        .jvmArgs("-server")
        .build();

    new Runner(opt).run();
  }

  private static String buildResultsFileName(final String outputDir,
      String resultFilePrefix, ResultFormatType resultType) {
    LocalDateTime date = LocalDateTime.now();
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM-dd-yyyy-hh-mm-ss");

    String suffix;
    switch (resultType) {
      case CSV:
        suffix = ".csv";
        break;
      case SCSV:
        // Semi-colon separated values
        suffix = ".scsv";
        break;
      case LATEX:
        suffix = ".tex";
        break;
      case JSON:
      default:
        suffix = ".json";
        break;

    }
    return new File(new File(outputDir),
        String.format("%s%s%s", resultFilePrefix, date.format(formatter), suffix)).getPath();
  }
}
