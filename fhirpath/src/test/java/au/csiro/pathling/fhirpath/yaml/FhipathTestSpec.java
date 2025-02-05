package au.csiro.pathling.fhirpath.yaml;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import lombok.Value;
import org.yaml.snakeyaml.Yaml;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;


@SuppressWarnings("unchecked")
@Value
public class FhipathTestSpec {


  private static final Yaml YAML_PARSER = new Yaml();

  @Value
  public static class TestCase {

    @Nullable
    String description;
    @Nonnull
    String expression;
    boolean error;
    @Nullable
    Object result;
  }

  @Nonnull
  Map<Object, Object> subject;

  @Nonnull
  List<TestCase> cases;


  @Nonnull
  static Stream<TestCase> mapCaseOrGroup(@Nonnull final Map<Object, Object> caseOrGroup) {
    if (caseOrGroup.containsKey("expression")) {
      return Stream.of(new TestCase(
          (String) caseOrGroup.get("desc"),
          (String) requireNonNull(caseOrGroup.get("expression")),
          (boolean) caseOrGroup.computeIfAbsent("error", k -> false),
          caseOrGroup.get("result")
      ));
    } else if (caseOrGroup.size() == 1) {
      final Object singleValue = caseOrGroup.values().iterator().next();
      return singleValue instanceof List<?> lst
             ? buildCases((List<Object>) lst).stream()
             : Stream.empty();
    } else {
      return Stream.empty();
    }
  }

  @Nonnull
  static List<TestCase> buildCases(@Nonnull final List<Object> cases) {
    return cases.stream()
        .map(c -> (Map<Object, Object>) c)
        .flatMap(FhipathTestSpec::mapCaseOrGroup)
        .toList();
  }

  @Nonnull
  static FhipathTestSpec fromYaml(@Nonnull final String yamlData) {
    Map<String, Object> yamlOM = YAML_PARSER.load(yamlData);
    return new FhipathTestSpec(
        (Map<Object, Object>) yamlOM.get("subject"),
        buildCases((List<Object>) requireNonNull(yamlOM.get("tests")))
    );
  }
}
