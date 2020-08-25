/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.fixtures;

import au.csiro.pathling.test.builders.DatasetBuilder;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

/**
 * @author Piotr Szul
 */

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

  public static DatasetBuilder allPatientsWithValue(final Object value) {
    return new DatasetBuilder().withColumn("id", DataTypes.StringType)
        .withColumn("value", DataTypes.BooleanType).withIdsAndValue(value, PATIENT_ALL_IDS);
  }
}
