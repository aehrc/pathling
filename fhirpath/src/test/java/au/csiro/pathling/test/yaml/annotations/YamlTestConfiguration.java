package au.csiro.pathling.test.yaml.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface YamlTestConfiguration {

  String config() default "";

  String resourceBase() default "";
}
