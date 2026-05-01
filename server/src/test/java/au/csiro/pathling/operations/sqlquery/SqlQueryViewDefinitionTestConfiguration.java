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
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.StringType;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/**
 * Test configuration that overrides the production {@code deltaLake} data source with an in-memory
 * one pre-loaded with both Patient resources and a ViewDefinition that selects against them. Used
 * by the {@code $sqlquery-run} end-to-end IT to drive a SQL query that resolves a ViewDefinition
 * reference and executes against real FHIR data — the path the production warehouse caching defeats
 * for resources created at runtime.
 */
@TestConfiguration
public class SqlQueryViewDefinitionTestConfiguration {

  /** The id of the pre-loaded ViewDefinition referenced by tests. */
  public static final String PATIENT_VIEW_ID = "patient-view";

  @Primary
  @Bean
  @Nonnull
  public QueryableDataSource deltaLake(
      @Nonnull final SparkSession sparkSession,
      @Nonnull final PathlingContext pathlingContext,
      @Nonnull final FhirEncoders fhirEncoders) {
    final List<IBaseResource> resources = new ArrayList<>();
    resources.add(patientView());
    resources.add(patient("p1", "Smith"));
    resources.add(patient("p2", "Johnson"));
    resources.add(patient("p3", "Williams"));
    return new CustomObjectDataSource(sparkSession, pathlingContext, fhirEncoders, resources);
  }

  @Nonnull
  private static ViewDefinitionResource patientView() {
    final ViewDefinitionResource view = new ViewDefinitionResource();
    view.setId(PATIENT_VIEW_ID);
    view.setName(new StringType("patient_view"));
    view.setResource(new CodeType("Patient"));
    view.setStatus(new CodeType("active"));
    final SelectComponent select = new SelectComponent();
    select.getColumn().add(column("id", "id"));
    select.getColumn().add(column("family_name", "name.first().family"));
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
}
