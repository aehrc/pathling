package au.csiro.pathling.views;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.SpringBootUnitTest;
import ca.uhn.fhir.context.FhirContext;
import com.google.gson.Gson;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;

@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class FhirViewTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  @Autowired
  FhirEncoders fhirEncoders;

  @Autowired
  Gson gson;

  @MockBean
  TerminologyServiceFactory terminologyServiceFactory;

  FhirViewExecutor executor;

  static final Path TEST_DATA_PATH = Path.of(
      "src/test/resources/test-data/views").toAbsolutePath().normalize();

  @BeforeEach
  void setUp() {
    final DataSource dataSource = Database.forFileSystem(spark, fhirEncoders,
        TEST_DATA_PATH.toUri().toString(), false);
    executor = new FhirViewExecutor(fhirContext, spark, dataSource,
        Optional.of(terminologyServiceFactory));
  }

  @Nonnull
  Stream<TestParameters> requests() throws IOException {
    final ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
    final Resource[] resources = resolver.getResources("classpath:requests/views/*.json");
    return Stream.of(resources).map(resource -> {
          try {
            return resource.getFile();
          } catch (final IOException e) {
            throw new RuntimeException(e);
          }
        }).map(File::toPath)
        .map(path -> {
          try {
            final FhirView view = gson.fromJson(new FileReader(path.toFile()), FhirView.class);
            return new TestParameters(view, path);
          } catch (final FileNotFoundException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @ParameterizedTest
  @MethodSource("requests")
  void test(@Nonnull final TestParameters parameters) {
    final Dataset<Row> result = executor.buildQuery(parameters.getView());
    assertThat(result)
        .hasRows(spark, "results/views/" +
            parameters.getRequestFile().getFileName().toString().replace(".json", ".csv"), true);
  }

  @Value
  static class TestParameters {

    FhirView view;
    Path requestFile;

    @Override
    public String toString() {
      return view.getName();
    }
  }

}
