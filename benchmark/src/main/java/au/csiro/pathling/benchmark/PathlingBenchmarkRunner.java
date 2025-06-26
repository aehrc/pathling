package au.csiro.pathling.benchmark;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.CommandLineOptions;

/**
 * Main class for running the benchmarks.
 *
 * @author John Grimes
 */
public class PathlingBenchmarkRunner {

  public static void main(final String[] args) throws Exception {
    final CommandLineOptions options = new CommandLineOptions(args);
    new Runner(options).run();
  }

}
