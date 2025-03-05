package au.csiro.pathling.test.yaml;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface YamlConfig {

  String config() default "";

  String resourceBase() default "";
}
