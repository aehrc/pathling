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
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * @author Piotr Szul
 */

@SuppressWarnings({"WeakerAccess", "unused"})
public class PatientResourceRowFixture {

  public static StructType createPatientRowStruct() {
    final Metadata metadata = new MetadataBuilder().build();
    final StructField genderColumn = new StructField("gender", DataTypes.StringType, true,
        metadata);
    final StructField activeColumn = new StructField("active", DataTypes.BooleanType, true,
        metadata);
    final StructType resourceStruct = new StructType(new StructField[]{genderColumn, activeColumn});
    final StructField id = new StructField("id", DataTypes.StringType, true, metadata);
    final StructField resource = new StructField("value", resourceStruct, true, metadata);
    return new StructType(new StructField[]{id, resource});
  }

  public static final StructType SCHEMA = createPatientRowStruct();

  public static final Row PATIENT_1_FEMALE =
      RowFactory.create("abc1", RowFactory.create("female", true));
  public static final Row PATIENT_2_FEMALE =
      RowFactory.create("abc2", RowFactory.create("female", false));
  public static final Row PATIENT_3_MALE =
      RowFactory.create("abc3", RowFactory.create("male", true));

  public final static List<String> PATIENT_ALL_IDS = Arrays.asList("abc1", "abc2", "abc3");

  public final static List<Row> PATIENT_ALL_ROWS =
      Arrays.asList(PATIENT_1_FEMALE, PATIENT_2_FEMALE, PATIENT_3_MALE);

  public final static List<Row> NO_ROWS = Collections.emptyList();

  public static Dataset<Row> createCompleteDataset(final SparkSession spark) {
    return spark.createDataFrame(PATIENT_ALL_ROWS, SCHEMA);
  }

  public static Dataset<Row> createEmptyDataset(final SparkSession spark) {
    return spark.createDataFrame(NO_ROWS, SCHEMA);
  }

  public static DatasetBuilder allPatientsWithValue(@Nonnull final SparkSession spark,
      @Nullable final Object value) {
    return new DatasetBuilder(spark).withColumn(DataTypes.StringType)
        .withColumn(DataTypes.BooleanType).withIdsAndValue(value, PATIENT_ALL_IDS);
  }
}
