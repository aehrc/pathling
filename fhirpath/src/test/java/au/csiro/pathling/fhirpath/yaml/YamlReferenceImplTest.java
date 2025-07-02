package au.csiro.pathling.fhirpath.yaml;

import au.csiro.pathling.test.yaml.YamlConfig;
import au.csiro.pathling.test.yaml.YamlSpec;
import au.csiro.pathling.test.yaml.YamlSpecCachedTestBase;
import au.csiro.pathling.test.yaml.runtimecase.RuntimeCase;
import jakarta.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;


@Slf4j
@Tag("YamlTest")
@YamlConfig(
    config = "fhirpath-js/config.yaml",
    resourceBase = "fhirpath-js/resources"
)

public class YamlReferenceImplTest extends YamlSpecCachedTestBase {

  //   From: https://github.com/hl7/fhirpath.js/
  //   Tag: 3.16.4
  //
  // 3.2_paths.yaml
  // 4.1_literals.yaml
  // 5.1_existence.yaml
  // 5.2_filtering_and_projection.yaml
  // 5.3_subsetting.yaml
  // 5.2.3_repeat.yaml
  // 5.4_combining.yaml
  // 5.5_conversion.yaml
  // 5.6_string_manipulation.yaml
  // 5.7_math.yaml
  // 5.8_tree_navigation.yaml
  // 5.9_utility_functions.yaml
  // 6.1_equality.yaml
  // 6.2_comparision.yaml
  // 6.3_types.yaml
  // 6.4_collection.yaml
  // 6.4_collections.yaml
  // 6.5_boolean_logic.yaml
  // 6.6_math.yaml
  // 7_aggregate.yaml
  // 8_variables.yaml

  @YamlSpec("fhirpath-js/cases/3.2_paths.yaml")
  void testPaths(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath-js/cases/4.1_literals.yaml")
  void testLiterals(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath-js/cases/5.1_existence.yaml")
  void testExistence(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath-js/cases/5.2.3_repeat.yaml")
  void testRepeat(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath-js/cases/5.2_filtering_and_projection.yaml")
  void testFilteringAndProjection(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath-js/cases/5.3_subsetting.yaml")
  void testSubsetting(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath-js/cases/5.4_combining.yaml")
  void testCombining(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath-js/cases/5.5_conversion.yaml")
  void testConversion(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath-js/cases/5.6_string_manipulation.yaml")
  void testStringManipulation(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath-js/cases/5.7_math.yaml")
  void testMath5(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath-js/cases/5.8_tree_navigation.yaml")
  void testTreeNavigation(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath-js/cases/5.9_utility_functions.yaml")
  void testUtilityFunctions(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath-js/cases/6.1_equality.yaml")
  void testEquality(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath-js/cases/6.2_comparision.yaml")
  void testComparison(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath-js/cases/6.3_types.yaml")
  void testTypes(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath-js/cases/6.4_collections.yaml")
  void testCollections(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath-js/cases/6.5_boolean_logic.yaml")
  void testBooleanLogic(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath-js/cases/6.6_math.yaml")
  void testMath6(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath-js/cases/7_aggregate.yaml")
  void testAggregate(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath-js/cases/8_variables.yaml")
  void testVariables(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  //
  // OTHER TEST CASES
  //

  //   extensions.yaml
  //   factory.yaml
  //   fhir-quantity.yaml
  //   fhir-r4.yaml
  //   hasValue.yaml
  //   simple.yaml

  @YamlSpec("fhirpath-js/cases/extensions.yaml")
  void testExtension(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath-js/cases/factory.yaml")
  void testFactory(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath-js/cases/fhir-r4.yaml")
  void testFhirR4(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath-js/cases/fhir-quantity.yaml")
  void testFhirQuantity(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath-js/cases/hasValue.yaml")
  void testHasValue(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath-js/cases/simple.yaml")
  void testSimple(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

}
