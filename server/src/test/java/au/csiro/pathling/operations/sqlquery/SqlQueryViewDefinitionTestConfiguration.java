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

package au.csiro.pathling.operations.sqlquery;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.encoders.ViewDefinitionResource.ColumnComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.SelectComponent;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.util.CustomObjectDataSource;
import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Observation.ObservationStatus;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.StringType;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/**
 * Test configuration that overrides the production {@code deltaLake} data source with an in-memory
 * one pre-loaded with FHIR resources and ViewDefinitions that select against them. Used by the
 * {@code $sqlquery-run} end-to-end IT to drive a SQL query that resolves a ViewDefinition reference
 * and executes against real FHIR data — the path the production warehouse caching defeats for
 * resources created at runtime.
 *
 * <p>Two views are exposed:
 *
 * <ul>
 *   <li>{@link #PATIENT_VIEW_ID} - a plain Patient view whose FHIRPath compiles to ordinary Spark
 *       expressions only (used by the baseline IT cases).
 *   <li>{@link #OBSERVATION_VIEW_ID} - an Observation view whose FHIRPath ({@code
 *       value.ofType(Quantity).value.toString()}) compiles to a call to the Pathling-registered
 *       {@code decimal_to_literal} UDF, exercising the case where the view-derived analyzed plan
 *       carries a {@code ScalaUDF} that user SQL must be allowed to select through.
 * </ul>
 */
@TestConfiguration
public class SqlQueryViewDefinitionTestConfiguration {

  /** The id of the pre-loaded Patient ViewDefinition referenced by tests. */
  public static final String PATIENT_VIEW_ID = "patient-view";

  /**
   * The canonical URL of the pre-loaded Patient ViewDefinition. The URL's final segment ({@code
   * Patients}) deliberately differs from the logical id ({@link #PATIENT_VIEW_ID}), so resolving a
   * dependency by this URL exercises the case the id-based resolution could not handle.
   */
  public static final String PATIENT_VIEW_URL =
      "https://pathling.csiro.au/test/ViewDefinition/Patients";

  /**
   * The id of the pre-loaded Observation ViewDefinition referenced by tests. Its FHIRPath uses
   * {@code .toString()} on a Decimal, which compiles to the Pathling-registered {@code
   * decimal_to_literal} UDF.
   */
  public static final String OBSERVATION_VIEW_ID = "observation-view";

  /** The canonical URL of the pre-loaded Observation ViewDefinition. */
  public static final String OBSERVATION_VIEW_URL =
      "https://pathling.csiro.au/test/ViewDefinition/Observations";

  @Primary
  @Bean
  @Nonnull
  public QueryableDataSource deltaLake(
      @Nonnull final SparkSession sparkSession,
      @Nonnull final PathlingContext pathlingContext,
      @Nonnull final FhirEncoders fhirEncoders) {
    final List<IBaseResource> resources = new ArrayList<>();
    resources.add(patientView());
    resources.add(observationView());
    resources.add(patient("p1", "Smith"));
    resources.add(patient("p2", "Johnson"));
    resources.add(patient("p3", "Williams"));
    resources.add(observation("o1", "p1", new BigDecimal("70.50")));
    resources.add(observation("o2", "p2", new BigDecimal("82.250")));
    resources.add(observation("o3", "p3", new BigDecimal("65")));
    return new CustomObjectDataSource(sparkSession, pathlingContext, fhirEncoders, resources);
  }

  @Nonnull
  private static ViewDefinitionResource patientView() {
    final ViewDefinitionResource view = new ViewDefinitionResource();
    view.setId(PATIENT_VIEW_ID);
    view.setUrl(PATIENT_VIEW_URL);
    view.setName(new StringType("patient_view"));
    view.setResource(new CodeType("Patient"));
    view.setStatus(new CodeType("active"));
    final SelectComponent select = new SelectComponent();
    select.getColumn().add(column("id", "id"));
    select.getColumn().add(column("family_name", "name.first().family"));
    view.getSelect().add(select);
    return view;
  }

  /**
   * View whose {@code weight_kg} column compiles to a call to {@code decimal_to_literal}, so the
   * resulting analyzed plan contains a Pathling {@code ScalaUDF}. Confirms that user SQL selecting
   * from this view is not blocked by the validator's "no non-built-in functions" rule, since that
   * rule only fires against {@code UnresolvedFunction}s in user-supplied SQL.
   */
  @Nonnull
  private static ViewDefinitionResource observationView() {
    final ViewDefinitionResource view = new ViewDefinitionResource();
    view.setId(OBSERVATION_VIEW_ID);
    view.setUrl(OBSERVATION_VIEW_URL);
    view.setName(new StringType("observation_view"));
    view.setResource(new CodeType("Observation"));
    view.setStatus(new CodeType("active"));
    final SelectComponent select = new SelectComponent();
    select.getColumn().add(column("id", "id"));
    select.getColumn().add(column("subject", "subject.reference"));
    select.getColumn().add(column("weight_kg", "value.ofType(Quantity).value.toString()"));
    view.getSelect().add(select);
    return view;
  }

  @Nonnull
  private static ColumnComponent column(@Nonnull final String name, @Nonnull final String path) {
    final ColumnComponent column = new ColumnComponent();
    column.setName(new StringType(name));
    column.setPath(new StringType(path));
    return column;
  }

  @Nonnull
  private static Patient patient(@Nonnull final String id, @Nonnull final String family) {
    final Patient patient = new Patient();
    patient.setId(id);
    patient.addName().setFamily(family);
    return patient;
  }

  @Nonnull
  private static Observation observation(
      @Nonnull final String id, @Nonnull final String patientId, @Nonnull final BigDecimal weight) {
    final Observation observation = new Observation();
    observation.setId(id);
    observation.setStatus(ObservationStatus.FINAL);
    observation.setSubject(new Reference("Patient/" + patientId));
    final Quantity quantity = new Quantity();
    quantity.setValue(weight);
    quantity.setUnit("kg");
    quantity.setSystem("http://unitsofmeasure.org");
    quantity.setCode("kg");
    observation.setValue(quantity);
    return observation;
  }
}
