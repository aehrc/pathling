/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.test.fixtures;

import au.csiro.pathling.test.builders.DatasetBuilder;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * @author Piotr Szul
 */
@SuppressWarnings("WeakerAccess")
public class PatientListBuilder {

  public static final String PATIENT_ID_121503c8 = "121503c8-9564-4b48-9086-a22df717948e";
  public static final String PATIENT_ID_2b36c1e2 = "2b36c1e2-bbe1-45ae-8124-4adad2677702";
  public static final String PATIENT_ID_7001ad9c = "7001ad9c-34d2-4eb5-8165-5fdc2147f469";
  public static final String PATIENT_ID_8ee183e2 = "8ee183e2-b3c0-4151-be94-b945d6aa8c6d";
  public static final String PATIENT_ID_9360820c = "9360820c-8602-4335-8b50-c88d627a0c20";
  public static final String PATIENT_ID_a7eb2ce7 = "a7eb2ce7-1075-426c-addd-957b861b0e55";
  public static final String PATIENT_ID_bbd33563 = "bbd33563-70d9-4f6d-a79a-dd1fc55f5ad9";
  public static final String PATIENT_ID_beff242e = "beff242e-580b-47c0-9844-c1a68c36c5bf";
  public static final String PATIENT_ID_e62e52ae = "e62e52ae-2d75-4070-a0ae-3cc78d35ed08";

  public static final List<String> PATIENT_ALL_IDS = Arrays.asList(PATIENT_ID_121503c8,
      PATIENT_ID_2b36c1e2, PATIENT_ID_7001ad9c, PATIENT_ID_8ee183e2, PATIENT_ID_9360820c,
      PATIENT_ID_a7eb2ce7, PATIENT_ID_bbd33563, PATIENT_ID_beff242e, PATIENT_ID_e62e52ae);

  public static DatasetBuilder allPatientsWithValue(@Nonnull final SparkSession spark,
      @Nullable final Boolean value) {
    return allPatientsWithValue(spark, DataTypes.BooleanType, value);
  }

  public static DatasetBuilder allPatientsWithValue(@Nonnull final SparkSession spark,
      @Nullable final Integer value) {
    return allPatientsWithValue(spark, DataTypes.IntegerType, value);
  }

  public static DatasetBuilder allPatientsWithValue(@Nonnull final SparkSession spark,
      @Nullable final Long value) {
    return allPatientsWithValue(spark, DataTypes.LongType, value);
  }

  public static DatasetBuilder allPatientsWithValue(@Nonnull final SparkSession spark,
      @Nullable final String value) {
    return allPatientsWithValue(spark, DataTypes.StringType, value);
  }

  public static DatasetBuilder allPatientsWithValue(@Nonnull final SparkSession spark,
      @Nonnull final DataType valueType, @Nullable final Object value) {
    return new DatasetBuilder(spark)
        .withColumn(DataTypes.StringType)
        .withColumn(valueType)
        .withIdsAndValue(value, PATIENT_ALL_IDS);
  }
}
