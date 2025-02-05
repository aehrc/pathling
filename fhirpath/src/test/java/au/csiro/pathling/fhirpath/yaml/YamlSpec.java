package au.csiro.pathling.fhirpath.yaml;

import au.csiro.pathling.fhirpath.yaml.YamlSpecTestBase.FhirpathArgumentProvider;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
@ArgumentsSource(FhirpathArgumentProvider.class)
@ParameterizedTest
public @interface YamlSpec {

  String value();
}
