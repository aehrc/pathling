package au.csiro.pathling.test.yaml.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotation to specify the configuration for YAML-based tests. This can include the path to a
 * configuration file and a base resource path.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface YamlTestConfiguration {

  String config() default "";

  String resourceBase() default "";
}
