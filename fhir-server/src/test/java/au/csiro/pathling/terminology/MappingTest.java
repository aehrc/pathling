package au.csiro.pathling.terminology;

import static au.csiro.pathling.test.assertions.Assertions.assertJson;

import ca.uhn.fhir.parser.IParser;
import java.nio.file.Path;
import java.nio.file.Paths;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public abstract class MappingTest {

  @Autowired
  protected IParser jsonParser;

  @Nullable
  protected TestInfo testInfo;

  @BeforeEach
  public void setUp(@Nonnull final TestInfo testInfo) {
    this.testInfo = testInfo;
  }

  protected void assertRequest(
      @Nonnull final IBaseResource resource) {
    //noinspection ConstantConditions,OptionalGetWithoutIsPresent
    assertRequest(resource, testInfo.getTestMethod().get().getName());
  }

  protected void assertRequest(
      @Nonnull final IBaseResource resource, @Nonnull final String name) {
    final String actualJson = jsonParser.encodeResourceToString(resource);

    //noinspection ConstantConditions,OptionalGetWithoutIsPresent
    final Path expectedPath = Paths
        .get("requests", testInfo.getTestClass().get().getSimpleName(),
            name + ".json");
    assertJson(expectedPath.toString(), actualJson);
  }

}
