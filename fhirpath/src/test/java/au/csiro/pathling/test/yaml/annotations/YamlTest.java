package au.csiro.pathling.test.yaml.annotations;

import au.csiro.pathling.test.yaml.YamlTestArgumentProvider;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Retention(RetentionPolicy.RUNTIME)
@ArgumentsSource(YamlTestArgumentProvider.class)
@ParameterizedTest
public @interface YamlTest {

  String value();
}
