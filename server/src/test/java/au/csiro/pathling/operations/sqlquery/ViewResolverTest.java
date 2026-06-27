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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.encoders.ViewDefinitionResource.ColumnComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.SelectComponent;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.test.SpringBootUnitTest;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Unit tests for {@link ViewResolver}, verifying that a {@code ViewDefinition} dependency resolves
 * by matching its canonical {@code url} (and {@code url|version}) - never its logical id - and that
 * the resolved canonical key is the resource's url plus its version.
 *
 * @author John Grimes
 */
@SpringBootUnitTest
class ViewResolverTest {

  private static final String PATIENTS_URL = "https://example.org/Patients";

  @Autowired private SparkSession spark;
  @Autowired private FhirEncoders fhirEncoders;
  @Autowired private FhirContext fhirContext;

  private DataSource dataSource;
  private ViewResolver resolver;

  @BeforeEach
  void setUp() {
    dataSource = mock(DataSource.class);
    final ServerConfiguration serverConfiguration = new ServerConfiguration();
    final AuthorizationConfiguration auth = new AuthorizationConfiguration();
    auth.setEnabled(false);
    serverConfiguration.setAuth(auth);
    resolver = new ViewResolver(dataSource, fhirEncoders, serverConfiguration, fhirContext);
  }

  @Test
  void resolvesAViewDefinitionByUrlNotLogicalId() {
    // The logical id ("vd-abc") deliberately differs from the URL's final segment ("Patients").
    when(dataSource.read("ViewDefinition"))
        .thenReturn(dataset(viewDefinition("vd-abc", PATIENTS_URL, null, "active", "Patient")));

    final Optional<ResolvedViewDefinition> resolved =
        resolver.resolveStoredViewDefinition(new ViewArtifactReference("patients", PATIENTS_URL));

    assertThat(resolved).isPresent();
    assertThat(resolved.get().getCanonicalKey()).isEqualTo(PATIENTS_URL);
    assertThat(resolved.get().getView().getResource()).isEqualTo("Patient");
  }

  @Test
  void returnsEmptyWhenNoStoredViewDefinitionMatchesTheUrl() {
    when(dataSource.read("ViewDefinition"))
        .thenReturn(dataset(viewDefinition("vd-abc", PATIENTS_URL, null, "active", "Patient")));

    final Optional<ResolvedViewDefinition> resolved =
        resolver.resolveStoredViewDefinition(
            new ViewArtifactReference("missing", "https://example.org/Missing"));

    assertThat(resolved).isEmpty();
  }

  @Test
  void selectsTheExactVersionWhenOneIsRequested() {
    when(dataSource.read("ViewDefinition"))
        .thenReturn(
            dataset(
                viewDefinition("v1", PATIENTS_URL, "1", "active", "Patient"),
                viewDefinition("v2", PATIENTS_URL, "2", "active", "Patient")));

    final Optional<ResolvedViewDefinition> resolved =
        resolver.resolveStoredViewDefinition(
            new ViewArtifactReference("patients", PATIENTS_URL + "|2"));

    assertThat(resolved).isPresent();
    assertThat(resolved.get().getCanonicalKey()).isEqualTo(PATIENTS_URL + "|2");
  }

  @Test
  void selectsTheLatestActiveVersionForABareUrl() {
    when(dataSource.read("ViewDefinition"))
        .thenReturn(
            dataset(
                viewDefinition("v1", PATIENTS_URL, "1", "retired", "Patient"),
                viewDefinition("v2", PATIENTS_URL, "2", "active", "Patient")));

    final Optional<ResolvedViewDefinition> resolved =
        resolver.resolveStoredViewDefinition(new ViewArtifactReference("patients", PATIENTS_URL));

    // The resolved canonical key is the chosen resource's url plus its version.
    assertThat(resolved).isPresent();
    assertThat(resolved.get().getCanonicalKey()).isEqualTo(PATIENTS_URL + "|2");
  }

  // ---------------------------------------------------------------------------
  // Helpers.
  // ---------------------------------------------------------------------------

  @Nonnull
  private Dataset<Row> dataset(@Nonnull final ViewDefinitionResource... views) {
    return spark
        .createDataset(List.of(views), fhirEncoders.of(ViewDefinitionResource.class))
        .toDF();
  }

  @Nonnull
  private static ViewDefinitionResource viewDefinition(
      @Nonnull final String id,
      @Nonnull final String url,
      @Nullable final String version,
      @Nonnull final String status,
      @Nonnull final String resourceType) {
    final ViewDefinitionResource view = new ViewDefinitionResource();
    view.setId(id);
    view.setUrl(url);
    if (version != null) {
      view.setVersion(version);
    }
    view.setName(new StringType(id + "_view"));
    view.setResource(new CodeType(resourceType));
    view.setStatus(new CodeType(status));
    final SelectComponent select = new SelectComponent();
    final ColumnComponent column = new ColumnComponent();
    column.setName(new StringType("id"));
    column.setPath(new StringType("id"));
    select.getColumn().add(column);
    view.getSelect().add(select);
    return view;
  }
}
