package au.csiro.pathling.schema;

import static au.csiro.pathling.test.TestResources.getResourceAsString;
import static au.csiro.pathling.test.TestResources.getResourceAsUrl;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.JSONException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

class FhirJsonReadWriteTest {

  private Path tempDirectory;

  // TODO: Add tests for other FHIR versions.
  @Nonnull
  private static Stream<TestParameters> parameters() {
    return Stream.of(
        new TestParameters("Patient", "fhir/json/anne.Patient.json",
            "schema/anne.Patient.schema.txt"),
        new TestParameters("Observation", "fhir/json/bodyTemp.Observation.json",
            "schema/bodyTemp.Observation.schema.txt"),
        new TestParameters("ExplanationOfBenefit", "fhir/json/withErrors.ExplanationOfBenefit.json",
            "schema/withErrors.ExplanationOfBenefit.schema.txt"),
        new TestParameters("ValueSet", "fhir/json/cpt.ValueSet.json",
            "schema/cpt.ValueSet.schema.txt"),
        new TestParameters("QuestionnaireResponse", "fhir/json/gcs.QuestionnaireResponse.json",
            "schema/gcs.QuestionnaireResponse.schema.txt"),
        new TestParameters("Binary", "fhir/json/photo.Binary.json",
            "schema/photo.Binary.schema.txt")
    );
  }

  @BeforeEach
  void setUp() throws IOException {
    tempDirectory = Files.createTempDirectory("pathling-FhirJsonReadWriteTest-");
  }

  @AfterEach
  void tearDown() throws IOException {
    deleteDirectoryRecursively(tempDirectory);
  }

  @ParameterizedTest
  @MethodSource("parameters")
  void test(@Nullable final TestParameters parameters) throws JSONException, IOException {
    assertNotNull(parameters);

    final SparkSession spark = SparkSession.builder()
        .master("local[*]")
        .getOrCreate();
    final String resourceUrl = getResourceAsUrl(parameters.resourceFile)
        .toString();
    final FhirJsonReader reader = new FhirJsonReader(parameters.resourceType, "R4",
        Map.of("multiLine", "true")
    );
    final FhirJsonWriter writer = new FhirJsonWriter("R4", parameters.resourceType);
    final Dataset<Row> data = reader.read(resourceUrl);

    final String expectedSchema = getResourceAsString(parameters.schemaFile);
    final String actualSchema = data.schema().treeString();
    assertEquals(expectedSchema, actualSchema);

    final Path targetPath = tempDirectory.resolve(parameters.resourceFile);
    final URI targetUri = targetPath.toUri();
    writer.write(data.repartition(1), targetUri.toString());
    final File singlePartition = getSinglePartition(targetPath, ".json");
    final String writtenJson = Files.readString(singlePartition.toPath());

    final String originalJson = getResourceAsString(parameters.resourceFile);
    JSONAssert.assertEquals(originalJson, writtenJson, JSONCompareMode.STRICT);
  }

  @Nonnull
  private static File getSinglePartition(@Nonnull final Path partitionedLocation,
      @Nonnull final String extension) throws IOException {
    try (final Stream<Path> stream = Files.list(partitionedLocation)) {
      final Path path = stream
          .filter(p -> p.toString().endsWith(extension))
          .findFirst()
          .orElseThrow(
              () -> new IllegalArgumentException("No files found in partitioned location"));
      return path.toFile();
    }
  }

  private static void deleteDirectoryRecursively(@Nonnull final Path path) throws IOException {
    Files.walkFileTree(path, new SimpleFileVisitor<>() {
      @Override
      public FileVisitResult visitFile(@Nullable final Path file,
          @Nullable final BasicFileAttributes attrs) throws IOException {
        if (file != null) {
          Files.delete(file);
        }
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(@Nullable final Path dir,
          @Nullable final IOException exc) throws IOException {
        if (dir != null) {
          Files.delete(dir);
        }
        return FileVisitResult.CONTINUE;
      }
    });
  }

  record TestParameters(@Nonnull String resourceType, @Nonnull String resourceFile,
                        @Nonnull String schemaFile) {

    @Override
    public String toString() {
      return resourceFile;
    }

  }

}
