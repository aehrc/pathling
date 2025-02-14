package au.csiro.pathling.test.yaml;

import static java.util.Objects.requireNonNull;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.yaml.snakeyaml.Yaml;


@SuppressWarnings("unchecked")
@Value
@Slf4j
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
    @Nullable
    String inputFile;
    @Nullable
    String model;
    @Nullable
    String context;
    boolean disable;
  }

  @Nullable
  Map<Object, Object> subject;

  @Nonnull
  List<TestCase> cases;


  @Nonnull
  static Stream<TestCase> mapCaseOrGroup(@Nonnull final Map<Object, Object> caseOrGroup) {
    if (caseOrGroup.containsKey("expression")) {
      // some cases contain multiple expressions,
      final List<String> expressions = toExpressions(requireNonNull(caseOrGroup.get("expression")));
      return expressions.stream()
          .map(expr -> new TestCase(
              (String) caseOrGroup.get("desc"),
              expr,
              (boolean) caseOrGroup.computeIfAbsent("error", k -> false),
              caseOrGroup.get("result"),
              (String) caseOrGroup.get("inputfile"),
              (String) caseOrGroup.get("model"),
              (String) caseOrGroup.get("context"),
              (boolean) caseOrGroup.computeIfAbsent("disable", k -> false)
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

  private static @Nonnull List<String> toExpressions(@Nonnull final Object expressionObj) {
    if (expressionObj instanceof String) {
      return List.of((String) expressionObj);
    } else if (expressionObj instanceof List<?>) {
      return (List<String>) expressionObj;
    } else {
      log.warn("Unexpected expression object: {}", expressionObj);
      return List.of("FAIL: " + expressionObj);
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
