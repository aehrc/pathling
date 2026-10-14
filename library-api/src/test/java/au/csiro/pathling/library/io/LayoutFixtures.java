/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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
package au.csiro.pathling.library.io;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.library.PathlingContext;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Patient;

/**
 * Builds resource data in each layout, and writes it where the sources and sinks look for it, for
 * the tests of layout detection (US7).
 *
 * <p>The previous layout is written by the previous encoder itself, with extensions enabled as the
 * public API enabled them, so that the data the tests refuse is exactly what an earlier release
 * stored. The Parquet on FHIR layout is written by encoding FHIR JSON through the public API.
 *
 * @author Piotr Szul
 */
public final class LayoutFixtures {

  /** The file name a resource type's table is stored under, as the file-based sinks name it. */
  @Nonnull public static final String PATIENT_TABLE = "Patient.parquet";

  /** The file name of the Condition table. */
  @Nonnull public static final String CONDITION_TABLE = "Condition.parquet";

  private LayoutFixtures() {}

  /**
   * Returns Patients in the layout written by earlier releases, through the previous encoder.
   *
   * @param spark the Spark session
   * @param ids the ids of the Patients
   * @return the Patients
   */
  @Nonnull
  public static Dataset<Row> previousPatients(
      @Nonnull final SparkSession spark, @Nonnull final String... ids) {
    final List<Patient> patients =
        List.of(ids).stream()
            .map(
                id -> {
                  final Patient patient = new Patient();
                  patient.setId(id);
                  patient.setGender(AdministrativeGender.FEMALE);
                  return patient;
                })
            .toList();
    return spark
        .createDataset(
            patients,
            FhirEncoders.forR4().withExtensionsEnabled(true).getOrCreate().of(Patient.class))
        .toDF();
  }

  /**
   * Returns Conditions in the layout written by earlier releases, through the previous encoder.
   *
   * @param spark the Spark session
   * @param ids the ids of the Conditions
   * @return the Conditions
   */
  @Nonnull
  public static Dataset<Row> previousConditions(
      @Nonnull final SparkSession spark, @Nonnull final String... ids) {
    final List<Condition> conditions =
        List.of(ids).stream()
            .map(
                id -> {
                  final Condition condition = new Condition();
                  condition.setId(id);
                  return condition;
                })
            .toList();
    return spark
        .createDataset(
            conditions,
            FhirEncoders.forR4().withExtensionsEnabled(true).getOrCreate().of(Condition.class))
        .toDF();
  }

  /**
   * Returns Patients in the Parquet on FHIR layout, encoded from FHIR JSON.
   *
   * @param context the Pathling context
   * @param ids the ids of the Patients
   * @return the Patients
   */
  @Nonnull
  public static Dataset<Row> newPatients(
      @Nonnull final PathlingContext context, @Nonnull final String... ids) {
    return encode(
        context,
        "Patient",
        List.of(ids).stream()
            .map(id -> "{\"resourceType\":\"Patient\",\"id\":\"" + id + "\",\"gender\":\"male\"}")
            .toList());
  }

  /**
   * Returns Conditions in the Parquet on FHIR layout, encoded from FHIR JSON.
   *
   * @param context the Pathling context
   * @param ids the ids of the Conditions
   * @return the Conditions
   */
  @Nonnull
  public static Dataset<Row> newConditions(
      @Nonnull final PathlingContext context, @Nonnull final String... ids) {
    return encode(
        context,
        "Condition",
        List.of(ids).stream()
            .map(
                id ->
                    "{\"resourceType\":\"Condition\",\"id\":\""
                        + id
                        + "\",\"subject\":{\"reference\":\"Patient/1\"}}")
            .toList());
  }

  /**
   * Writes a dataset as a Parquet table, where the Parquet source looks for it.
   *
   * @param dataset the data
   * @param directory the directory the source reads
   * @param tableName the file name of the table within it
   */
  public static void writeParquet(
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final Path directory,
      @Nonnull final String tableName) {
    dataset.write().mode("append").parquet(directory.resolve(tableName).toString());
  }

  /**
   * Writes a dataset as a Delta table, where the Delta source looks for it.
   *
   * @param dataset the data
   * @param directory the directory the source reads
   * @param tableName the file name of the table within it
   */
  public static void writeDelta(
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final Path directory,
      @Nonnull final String tableName) {
    dataset.write().format("delta").save(directory.resolve(tableName).toString());
  }

  @Nonnull
  private static Dataset<Row> encode(
      @Nonnull final PathlingContext context,
      @Nonnull final String resourceType,
      @Nonnull final List<String> json) {
    return context.encode(
        context.getSpark().createDataset(json, Encoders.STRING()).toDF(), resourceType);
  }
}
