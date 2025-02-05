package au.csiro.pathling.fhirpath.yaml;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import lombok.Value;
import org.yaml.snakeyaml.Yaml;
import java.util.List;
import java.util.Map;

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
  static List<TestCase> buildCases(@Nonnull final List<Object> cases) {
    return cases.stream()
        .map(c -> (Map<Object, Object>) c)
        .filter(caseMap -> caseMap.containsKey("expression"))
        .map(caseMap -> new TestCase(
            (String) caseMap.get("desc"),
            (String) requireNonNull(caseMap.get("expression")),
            (boolean) caseMap.computeIfAbsent("error", k -> false),
            caseMap.get("result")
        ))
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
