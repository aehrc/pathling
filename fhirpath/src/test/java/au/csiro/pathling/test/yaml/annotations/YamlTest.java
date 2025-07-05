package au.csiro.pathling.test.yaml.annotations;

import au.csiro.pathling.test.yaml.YamlTestArgumentProvider;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Designates a method as a parameterized test that will be executed with test cases defined in a
 * YAML file.
 */
@Retention(RetentionPolicy.RUNTIME)
@ArgumentsSource(YamlTestArgumentProvider.class)
@ParameterizedTest
public @interface YamlTest {

  String value();
}
