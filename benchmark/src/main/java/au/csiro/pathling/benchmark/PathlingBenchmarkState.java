package au.csiro.pathling.benchmark;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.DatasetSource;
import au.csiro.pathling.library.io.source.DeltaSource;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Benchmark)
public class PathlingBenchmarkState {

  private static final List<String> VIEW_DEFINITIONS = List.of(
      "ConditionFlat", "EncounterFlat", "PatientAddresses", "PatientAndContactAddressUnion",
      "PatientDemographics", "UsCoreBloodPressures"
  );

  @Nonnull
  private final PathlingContext pathlingContext;

  @Nonnull
  private final QueryableDataSource dataSource;

  @Nonnull
  private final Map<String, String> viewDefinitions;

  @Param({"ndjson", "delta"})
  private String sourceType;

  @SuppressWarnings("ConstantValue")
  public PathlingBenchmarkState() {
    final SparkSession spark = SparkSession.builder()
        .appName("PathlingBenchmark")
        .master("local[*]")
        .getOrCreate();
    this.pathlingContext = PathlingContext.create(spark);

    final List<String> resourceTypes = List.of("Patient", "Observation", "Condition", "Encounter");
    if ("ndjson".equals(sourceType)) {
      this.dataSource = initialiseNdjsonSource(resourceTypes);
    } else if ("delta".equals(sourceType)) {
      this.dataSource = initialiseDeltaSource(resourceTypes);
    } else {
      throw new IllegalArgumentException("Unknown source type: " + sourceType);
    }

    this.viewDefinitions = VIEW_DEFINITIONS.stream()
        .collect(toMap(
            viewDefName -> viewDefName,
            viewDefName -> {
              try (final InputStream in = getResourceAsStream(viewDefName + ".json")) {
                return new String(in.readAllBytes(), StandardCharsets.UTF_8);
              } catch (final Exception e) {
                throw new RuntimeException("Failed to read view definition: " + viewDefName, e);
              }
            }
        ));
  }

  private @Nonnull DatasetSource initialiseNdjsonSource(
      @Nonnull final Iterable<String> resourceTypes) {
    // Create a DatasetSource.
    final DatasetSource datasetSource = pathlingContext.read().datasets();
    for (final String resourceType : resourceTypes) {
      // Load the NDJSON file for the resource type and encode it.
      final Path ndjsonPath = extractResourceToTempFile("bulk/fhir/" + resourceType + ".ndjson");
      final Dataset<Row> strings = this.pathlingContext.getSpark().read().format("text")
          .load(ndjsonPath.toString());
      final Dataset<Row> encoded = pathlingContext.encode(strings, resourceType);
      // Register the dataset with the DatasetSource.
      datasetSource.dataset(resourceType, encoded);
    }
    return datasetSource;
  }

  private @Nonnull DeltaSource initialiseDeltaSource(
      @Nonnull final Iterable<String> resourceTypes) {
    final DatasetSource datasetSource = initialiseNdjsonSource(resourceTypes);
    // Create a temporary directory for the Delta tables.
    final Path tempDir;
    try {
      tempDir = Files.createTempDirectory("pathling-benchmark-delta-");
      tempDir.toFile().deleteOnExit();
    } catch (final Exception e) {
      throw new RuntimeException("Failed to create temporary directory for Delta tables", e);
    }
    // Write each dataset to a Delta table in the temporary directory.
    datasetSource.write().delta(tempDir.toString());
    // Create a DeltaSource that reads from the temporary directory.
    return pathlingContext.read().delta(tempDir.toString());
  }

  private static ClassLoader getClassLoader() {
    final ClassLoader object = Thread.currentThread().getContextClassLoader();
    return requireNonNull(object);
  }

  private static InputStream getResourceAsStream(final String name) {
    final ClassLoader loader = getClassLoader();
    final InputStream inputStream = loader.getResourceAsStream(name);
    requireNonNull(inputStream, "Test resource not found: " + name);
    return inputStream;
  }

  @Nonnull
  private static Path extractResourceToTempFile(final String resourceName) {
    try (final InputStream in = getResourceAsStream(resourceName)) {
      final Path tempFile = Files.createTempFile("pathling-benchmark-",
          "-" + resourceName.replace('/', '_'));
      tempFile.toFile().deleteOnExit();
      try (final OutputStream out = Files.newOutputStream(tempFile)) {
        in.transferTo(out);
      }
      return tempFile;
    } catch (final Exception e) {
      throw new RuntimeException("Failed to extract resource: " + resourceName, e);
    }
  }

  @Nonnull
  public PathlingContext getPathlingContext() {
    return pathlingContext;
  }

  @Nonnull
  public QueryableDataSource getNdjsonSource() {
    return dataSource;
  }

  @Nonnull
  public Map<String, String> getViewDefinitions() {
    return viewDefinitions;
  }

  @TearDown(Level.Trial)
  public void teardown() {
    pathlingContext.getSpark().stop();
  }

}
