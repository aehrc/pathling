/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.utilities;

import static au.csiro.clinsight.utilities.Strings.capitalize;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author John Grimes
 */
public abstract class Configuration {

  private static final Logger logger = LoggerFactory.getLogger(Configuration.class);

  public static <T> T setStringPropsUsingEnvVar(T target,
      Map<String, String> envVarToPropertyNames) {
    for (String envVarName : envVarToPropertyNames.keySet()) {
      try {
        Method method = target.getClass()
            .getDeclaredMethod("set" + capitalize(envVarToPropertyNames.get(envVarName)),
                String.class);
        String value = System.getenv(envVarName);
        if (value != null) {
          method.invoke(target, value);
        }
      } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
        logger.error(
            "Failed to set property " + envVarToPropertyNames.get(envVarName)
                + " from environment variable " + envVarName + ": " + e.getMessage());
      }
    }
    return target;
  }

  public static <T> T copyStringProps(Object source, T target, List<String> propertyNames) {
    for (String propertyName : propertyNames) {
      try {
        String capitalizedName = capitalize(propertyName);
        Method getterMethod;
        getterMethod = source.getClass()
            .getDeclaredMethod("get" + capitalizedName);
        Method setterMethod = target.getClass()
            .getDeclaredMethod("set" + capitalizedName, String.class);
        String value = (String) getterMethod.invoke(source);
        if (value != null) {
          setterMethod.invoke(target, value);
        }
      } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
        logger.error("Failed to copy property " + propertyName + ": " + e.getMessage());
      }
    }
    return target;
  }

}
