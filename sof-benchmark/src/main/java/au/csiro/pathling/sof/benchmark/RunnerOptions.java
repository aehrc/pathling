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

package au.csiro.pathling.sof.benchmark;

import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Parsed command-line options for {@link SofBenchmarkRunner}: the positional benchmark file plus
 * the {@code --size}, {@code --data}, {@code --sink} and {@code --out} flags.
 */
public class RunnerOptions {

  @Nonnull private static final String DEFAULT_SINK = "csv";

  @Nonnull private static final String DEFAULT_OUT = "benchmark-report.json";

  @Nonnull private final Path benchmarkFile;

  @Nonnull private final String size;

  @Nonnull private final Path dataRoot;

  @Nonnull private final String sink;

  @Nonnull private final Path out;

  private RunnerOptions(
      @Nonnull final Path benchmarkFile,
      @Nonnull final String size,
      @Nonnull final Path dataRoot,
      @Nonnull final String sink,
      @Nonnull final Path out) {
    this.benchmarkFile = benchmarkFile;
    this.size = size;
    this.dataRoot = dataRoot;
    this.sink = sink;
    this.out = out;
  }

  /**
   * Parses the runner's command-line arguments.
   *
   * @param args the command-line arguments
   * @return the parsed options
   * @throws IllegalArgumentException if a required option is missing or an unknown option is given
   */
  @Nonnull
  public static RunnerOptions parse(@Nonnull final String[] args) {
    Path benchmarkFile = null;
    String size = null;
    Path dataRoot = null;
    String sink = DEFAULT_SINK;
    Path out = Path.of(DEFAULT_OUT);

    for (int i = 0; i < args.length; i++) {
      final String arg = args[i];
      switch (arg) {
        case "--size" -> size = requireValue(args, ++i, "--size");
        case "--data" -> dataRoot = Path.of(requireValue(args, ++i, "--data"));
        case "--sink" -> sink = requireValue(args, ++i, "--sink");
        case "--out" -> out = Path.of(requireValue(args, ++i, "--out"));
        default -> {
          if (arg.startsWith("--")) {
            throw new IllegalArgumentException("Unknown option: " + arg + ". " + usage());
          }
          if (benchmarkFile != null) {
            throw new IllegalArgumentException(
                "Unexpected extra argument: " + arg + ". " + usage());
          }
          benchmarkFile = Path.of(arg);
        }
      }
    }

    if (benchmarkFile == null) {
      throw new IllegalArgumentException("Missing required benchmark file. " + usage());
    }
    if (dataRoot == null) {
      throw new IllegalArgumentException("Missing required option --data. " + usage());
    }
    final Path resolvedBenchmarkFile = benchmarkFile;
    final String resolvedSize =
        Optional.ofNullable(size)
            .orElseThrow(
                () -> new IllegalArgumentException("Missing required option --size. " + usage()));

    return new RunnerOptions(resolvedBenchmarkFile, resolvedSize, dataRoot, sink, out);
  }

  @Nonnull
  private static String requireValue(
      @Nonnull final String[] args, final int index, @Nonnull final String option) {
    if (index >= args.length) {
      throw new IllegalArgumentException("Option " + option + " requires a value. " + usage());
    }
    return args[index];
  }

  @Nonnull
  private static String usage() {
    return "Usage: SofBenchmarkRunner <benchmarkFile> --size <size> --data <dataRoot>"
        + " [--sink csv|parquet] [--out report.json]";
  }

  /**
   * Returns the path to the benchmark file.
   *
   * @return the benchmark file
   */
  @Nonnull
  public Path getBenchmarkFile() {
    return benchmarkFile;
  }

  /**
   * Returns the requested size key.
   *
   * @return the size key
   */
  @Nonnull
  public String getSize() {
    return size;
  }

  /**
   * Returns the data root directory.
   *
   * @return the data root
   */
  @Nonnull
  public Path getDataRoot() {
    return dataRoot;
  }

  /**
   * Returns the Spark write format used for the timed region.
   *
   * @return the sink format
   */
  @Nonnull
  public String getSink() {
    return sink;
  }

  /**
   * Returns the report output path.
   *
   * @return the output path
   */
  @Nonnull
  public Path getOut() {
    return out;
  }
}
