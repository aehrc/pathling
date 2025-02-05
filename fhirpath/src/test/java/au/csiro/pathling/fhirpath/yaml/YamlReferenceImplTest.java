package au.csiro.pathling.fhirpath.yaml;

import jakarta.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;


@Slf4j
@Tag("WorkTest")
@Disabled
public class YamlReferenceImplTest extends YamlSpecTestBase {


  @YamlSpec("fhirpath/cases/5.1_existence.yaml")
  void testExistence(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath/cases/6.1_equality.yaml")
  void testEquality(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath/cases/6.4_collections.yaml")
  void testCollections(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath/cases/6.5_boolean_logic.yaml")
  void testBooleanLogic(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }
}
