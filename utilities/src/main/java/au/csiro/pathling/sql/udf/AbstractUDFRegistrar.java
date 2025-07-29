/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.sql.udf;

import au.csiro.pathling.spark.SparkConfigurer;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.UDFRegistration;

/**
 * A base Spark configurer that registers user-defined functions (UDFs) in Spark sessions.
 * <p>
 * This abstract class provides a standardised way to register SQL functions with Spark sessions.
 * Subclasses should implement the {@link #registerUDFs(UDFRegistrar)} method to define which
 * functions to register.
 * </p>
 * <p>
 * The class implements the {@link SparkConfigurer} interface, allowing it to be used as part of
 * the Spark session configuration process.
 * </p>
 *
 * @author Piotr Szul
 * @author John Grimes
 * @see SparkConfigurer
 * @see SqlFunction1
 * @see SqlFunction2
 */
public abstract class AbstractUDFRegistrar implements SparkConfigurer {

  /**
   * A helper class for registering user-defined functions with a Spark session.
   * <p>
   * This class provides convenient methods for registering different types of SQL functions
   * and maintains a fluent interface for chaining multiple registrations.
   * </p>
   */
  protected static class UDFRegistrar {

    private final UDFRegistration udfRegistration;

    /**
     * Creates a new UDF registrar for the given Spark session.
     *
     * @param spark the Spark session to register functions with
     */
    public UDFRegistrar(@Nonnull final SparkSession spark) {
      this.udfRegistration = spark.udf();
    }

    /**
     * Registers a single-argument SQL function with the Spark session.
     *
     * @param udf1 the function to register
     * @return this registrar for method chaining
     */
    public UDFRegistrar register(@Nonnull final SqlFunction1<?, ?> udf1) {
      udfRegistration.register(udf1.getName(), udf1, udf1.getReturnType());
      return this;
    }

    /**
     * Registers a two-argument SQL function with the Spark session.
     *
     * @param udf2 the function to register
     * @return this registrar for method chaining
     */
    public UDFRegistrar register(@Nonnull final SqlFunction2<?, ?, ?> udf2) {
      udfRegistration.register(udf2.getName(), udf2, udf2.getReturnType());
      return this;
    }

  }

  /**
   * Configures the Spark session by registering user-defined functions.
   * <p>
   * This method is called as part of the Spark session configuration process and delegates
   * to the abstract {@link #registerUDFs(UDFRegistrar)} method to perform the actual
   * function registration.
   * </p>
   *
   * @param spark the Spark session to configure
   */
  @Override
  public void configure(@Nonnull final SparkSession spark) {
    registerUDFs(new UDFRegistrar(spark));
  }

  /**
   * Override to perform the actual registration of SqlFunctionXXX instances.
   *
   * @param udfRegistrar the helper to use to register the UDFs.
   */
  protected abstract void registerUDFs(@Nonnull final UDFRegistrar udfRegistrar);
}
