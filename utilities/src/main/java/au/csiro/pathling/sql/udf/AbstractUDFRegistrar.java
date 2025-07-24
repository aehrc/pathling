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
 * A base spark configurer that registers user defined functions in the sessions.
 *
 * @author Piotr Szul
 */
public abstract class AbstractUDFRegistrar implements SparkConfigurer {

  protected static class UDFRegistrar {

    private final UDFRegistration udfRegistration;

    public UDFRegistrar(@Nonnull SparkSession spark) {
      this.udfRegistration = spark.udf();
    }

    public UDFRegistrar register(@Nonnull SqlFunction1<?, ?> udf1) {
      udfRegistration.register(udf1.getName(), udf1, udf1.getReturnType());
      return this;
    }

    public UDFRegistrar register(@Nonnull SqlFunction2<?, ?, ?> udf2) {
      udfRegistration.register(udf2.getName(), udf2, udf2.getReturnType());
      return this;
    }

  }

  @Override
  public void configure(@Nonnull final SparkSession spark) {
    registerUDFs(new UDFRegistrar(spark));
  }

  /**
   * Override to perform the actual registration of SqlFunctionXXX instances.
   *
   * @param udfRegistrar the helper to use to register the UDFs.
   */
  abstract protected void registerUDFs(@Nonnull final UDFRegistrar udfRegistrar);
}
