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

package au.csiro.pathling.library.io.source;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.fhir.export.BulkExportClient;
import au.csiro.fhir.export.BulkExportResult;
import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.io.FhirReader;
import au.csiro.pathling.library.PathlingContext;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

/**
 * The fixture of the tests of sources given a schema set: Patients that differ from a defined
 * Patient schema in both directions, and a Condition the schema set does not name, held by each
 * kind of source.
 *
 * <p>The defined schema has {@code gender}, {@code birthDate} and {@code name.family}. The first
 * Patient lacks {@code birthDate} and {@code name.family}, and carries {@code active} and {@code
 * name.given}, which the schema lacks. The second lacks {@code gender} and {@code name}, and
 * carries {@code active}.
 */
final class SchemaSetSources {

  /** The resource type the schema set names. */
  @Nonnull static final String PATIENT = "Patient";

  /** A resource type the source holds and the schema set does not name. */
  @Nonnull static final String CONDITION = "Condition";

  /** The source that reads NDJSON files. */
  @Nonnull static final String NDJSON = "ndjson";

  /** The source that reads JSON bundles. */
  @Nonnull static final String BUNDLES_JSON = "bundlesJson";

  /** The source that reads XML bundles. */
  @Nonnull static final String BUNDLES_XML = "bundlesXml";

  /** The source that reads Parquet files. */
  @Nonnull static final String PARQUET = "parquet";

  /** The source that reads Delta tables. */
  @Nonnull static final String DELTA = "delta";

  /** The source that reads catalog tables. */
  @Nonnull static final String TABLES = "tables";

  /** The source that reads the NDJSON output of a bulk export. */
  @Nonnull static final String BULK = "bulk";

  /** The source that holds datasets. */
  @Nonnull static final String DATASETS = "datasets";

  @Nonnull
  private static final List<String> PATIENT_JSON =
      List.of(
          "{\"resourceType\":\"Patient\",\"id\":\"p1\",\"active\":true,\"gender\":\"female\","
              + "\"name\":[{\"given\":[\"Ann\"]}]}",
          "{\"resourceType\":\"Patient\",\"id\":\"p2\",\"active\":false,"
              + "\"birthDate\":\"1990-01-01\"}");

  @Nonnull
  private static final List<String> PATIENT_XML =
      List.of(
          "<Patient xmlns=\"http://hl7.org/fhir\"><id value=\"p1\"/><active value=\"true\"/>"
              + "<name><given value=\"Ann\"/></name><gender value=\"female\"/></Patient>",
          "<Patient xmlns=\"http://hl7.org/fhir\"><id value=\"p2\"/><active value=\"false\"/>"
              + "<birthDate value=\"1990-01-01\"/></Patient>");

  @Nonnull
  private static final String CONDITION_JSON =
      "{\"resourceType\":\"Condition\",\"id\":\"c1\",\"subject\":{\"reference\":\"Patient/p1\"}}";

  @Nonnull
  private static final String CONDITION_XML =
      "<Condition xmlns=\"http://hl7.org/fhir\"><id value=\"c1\"/>"
          + "<subject><reference value=\"Patient/p1\"/></subject></Condition>";

  /** A Patient carrying every element of the defined schema, from which it is read. */
  @Nonnull
  private static final String SCHEMA_DONOR_JSON =
      "{\"resourceType\":\"Patient\",\"id\":\"d\",\"gender\":\"male\","
          + "\"birthDate\":\"2000-01-01\",\"name\":[{\"family\":\"Smith\"}]}";

  private SchemaSetSources() {}

  /**
   * Returns the io reader that a context's sources read with.
   *
   * @param context the Pathling context
   * @return the reader
   */
  @Nonnull
  static FhirReader reader(@Nonnull final PathlingContext context) {
    return FhirReader.of(
        context.getSpark(),
        FhirDefinitionContext.of(context.getFhirContext()),
        context.getSchemaConfiguration());
  }

  /**
   * Returns the defined Patient schema: a PoF schema with {@code gender}, {@code birthDate} and
   * {@code name.family}, without annotations.
   *
   * @param context the Pathling context
   * @return the schema
   */
  @Nonnull
  static StructType definedPatientSchema(@Nonnull final PathlingContext context) {
    return reader(context)
        .json()
        .read(PATIENT, strings(context, List.of(SCHEMA_DONOR_JSON)))
        .schema();
  }

  /**
   * Returns the schema a load with a defined schema should give under a context's configuration:
   * the io defined read of no documents, annotated as the configuration says.
   *
   * @param context the Pathling context
   * @param resourceType the resource type
   * @param schema the defined schema
   * @return the expected schema, with its annotations
   */
  @Nonnull
  static StructType expectedSchema(
      @Nonnull final PathlingContext context,
      @Nonnull final String resourceType,
      @Nonnull final StructType schema) {
    return context
        .annotate(
            reader(context).json().read(resourceType, strings(context, List.of()), schema),
            resourceType)
        .schema();
  }

  /**
   * Returns the Patients encoded without a schema, as a stored source holds them.
   *
   * @param context the Pathling context
   * @return the Patients
   */
  @Nonnull
  static Dataset<Row> encodedPatients(@Nonnull final PathlingContext context) {
    return context.encode(strings(context, PATIENT_JSON).toDF(), PATIENT);
  }

  /**
   * Returns the Conditions encoded without a schema, as a stored source holds them.
   *
   * @param context the Pathling context
   * @return the Conditions
   */
  @Nonnull
  static Dataset<Row> encodedConditions(@Nonnull final PathlingContext context) {
    return context.encode(strings(context, List.of(CONDITION_JSON)).toDF(), CONDITION);
  }

  /**
   * Writes the fixture where a kind of source reads it. A dataset source needs nothing written.
   *
   * @param kind the kind of source
   * @param context the Pathling context
   * @param directory the directory a file-based source reads
   * @param database the catalog database the table source reads
   */
  static void write(
      @Nonnull final String kind,
      @Nonnull final PathlingContext context,
      @Nonnull final Path directory,
      @Nonnull final String database) {
    switch (kind) {
      case NDJSON, BULK -> {
        writeFile(directory.resolve("Patient.ndjson"), String.join("\n", PATIENT_JSON));
        writeFile(directory.resolve("Condition.ndjson"), CONDITION_JSON);
      }
      case BUNDLES_JSON -> writeFile(directory.resolve("bundle.json"), jsonBundle());
      case BUNDLES_XML -> writeFile(directory.resolve("bundle.xml"), xmlBundle());
      case PARQUET -> {
        encodedPatients(context).write().parquet(directory.resolve("Patient.parquet").toString());
        encodedConditions(context)
            .write()
            .parquet(directory.resolve("Condition.parquet").toString());
      }
      case DELTA -> {
        encodedPatients(context)
            .write()
            .format("delta")
            .save(directory.resolve("Patient.parquet").toString());
        encodedConditions(context)
            .write()
            .format("delta")
            .save(directory.resolve("Condition.parquet").toString());
      }
      case TABLES -> {
        encodedPatients(context).write().format("delta").saveAsTable(database + "." + PATIENT);
        encodedConditions(context).write().format("delta").saveAsTable(database + "." + CONDITION);
      }
      case DATASETS -> {
        // A dataset source is given its datasets when it is opened.
      }
      default -> throw new IllegalArgumentException("Unknown source: " + kind);
    }
  }

  /**
   * Opens a kind of source over the fixture that {@link #write} wrote, from a builder.
   *
   * @param kind the kind of source
   * @param builder the builder, with or without a schema set
   * @param directory the directory a file-based source reads
   * @param database the catalog database the table source reads
   * @return the source
   */
  @Nonnull
  static QueryableDataSource open(
      @Nonnull final String kind,
      @Nonnull final DataSourceBuilder builder,
      @Nonnull final Path directory,
      @Nonnull final String database) {
    final PathlingContext context = builder.context();
    return switch (kind) {
      case NDJSON -> builder.ndjson(directory.toString());
      case BUNDLES_JSON ->
          builder.bundles(
              directory.toString(), Set.of(PATIENT, CONDITION), PathlingContext.FHIR_JSON);
      case BUNDLES_XML ->
          builder.bundles(
              directory.toString(), Set.of(PATIENT, CONDITION), PathlingContext.FHIR_XML);
      case PARQUET -> builder.parquet(directory.toString());
      case DELTA -> builder.delta(directory.toString());
      case TABLES -> builder.tables(database);
      case BULK -> builder.bulk(bulkClient(directory));
      case DATASETS ->
          builder
              .datasets()
              .dataset(PATIENT, encodedPatients(context))
              .dataset(CONDITION, encodedConditions(context));
      default -> throw new IllegalArgumentException("Unknown source: " + kind);
    };
  }

  /** Returns a bulk export client whose export has already left its NDJSON in a directory. */
  @Nonnull
  private static BulkExportClient bulkClient(@Nonnull final Path directory) {
    final BulkExportClient client = mock(BulkExportClient.class);
    when(client.export()).thenReturn(BulkExportResult.of(Instant.now(), List.of()));
    when(client.getOutputDir()).thenReturn(directory.toString());
    return client;
  }

  /**
   * Returns the Patients as FHIR JSON documents, one per row.
   *
   * @param context the Pathling context
   * @return the documents
   */
  @Nonnull
  static Dataset<Row> patientJson(@Nonnull final PathlingContext context) {
    return strings(context, PATIENT_JSON).toDF();
  }

  /**
   * Returns the Patients as FHIR XML documents, one per row.
   *
   * @param context the Pathling context
   * @return the documents
   */
  @Nonnull
  static Dataset<Row> patientXml(@Nonnull final PathlingContext context) {
    return strings(context, PATIENT_XML).toDF();
  }

  /**
   * Returns a FHIR JSON bundle holding the Patients and the Condition, in a dataset of one row.
   *
   * @param context the Pathling context
   * @return the bundle
   */
  @Nonnull
  static Dataset<Row> jsonBundles(@Nonnull final PathlingContext context) {
    return strings(context, List.of(jsonBundle())).toDF();
  }

  /**
   * Returns a FHIR XML bundle holding the Patients and the Condition, in a dataset of one row.
   *
   * @param context the Pathling context
   * @return the bundle
   */
  @Nonnull
  static Dataset<Row> xmlBundles(@Nonnull final PathlingContext context) {
    return strings(context, List.of(xmlBundle())).toDF();
  }

  @Nonnull
  private static String jsonBundle() {
    final List<String> resources =
        List.of(PATIENT_JSON.get(0), PATIENT_JSON.get(1), CONDITION_JSON);
    return "{\"resourceType\":\"Bundle\",\"type\":\"collection\",\"entry\":["
        + String.join(",", resources.stream().map(r -> "{\"resource\":" + r + "}").toList())
        + "]}";
  }

  @Nonnull
  private static String xmlBundle() {
    final List<String> resources = List.of(PATIENT_XML.get(0), PATIENT_XML.get(1), CONDITION_XML);
    return "<Bundle xmlns=\"http://hl7.org/fhir\"><type value=\"collection\"/>"
        + String.join(
            "",
            resources.stream().map(r -> "<entry><resource>" + r + "</resource></entry>").toList())
        + "</Bundle>";
  }

  @Nonnull
  private static Dataset<String> strings(
      @Nonnull final PathlingContext context, @Nonnull final List<String> values) {
    return context.getSpark().createDataset(values, Encoders.STRING());
  }

  private static void writeFile(@Nonnull final Path file, @Nonnull final String content) {
    try {
      Files.writeString(file, content);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
