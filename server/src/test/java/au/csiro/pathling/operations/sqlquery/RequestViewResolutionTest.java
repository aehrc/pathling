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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.encoders.ViewDefinitionResource.ColumnComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.SelectComponent;
import au.csiro.pathling.operations.sql.ContextArtefactParser;
import au.csiro.pathling.operations.sql.FhirViewValidator;
import au.csiro.pathling.operations.sql.SuppliedArtefact;
import au.csiro.pathling.operations.sql.SuppliedArtefacts;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests for the resolution of request-supplied views against canonical dependency references: a
 * supplied view is matched to a reference by its {@code url} (by {@link SuppliedArtefacts}), while
 * a supplied view that carries no {@code url} is rejected at parse time (by {@link
 * ContextArtefactParser}).
 *
 * @author John Grimes
 */
class RequestViewResolutionTest {

  private static final String PATIENTS_URL = "https://example.org/Patients";

  // ---------------------------------------------------------------------------
  // Supplied-view matching by url (ViewResolver).
  // ---------------------------------------------------------------------------

  @Nested
  class SuppliedViewMatching {

    @Test
    void suppliedViewIsMatchedByUrl() {
      final var supplied =
          au.csiro.pathling.views.FhirView.ofResource("Patient")
              .select(
                  au.csiro.pathling.views.FhirView.columns(
                      au.csiro.pathling.views.FhirView.column("id", "id")))
              .build();
      final SuppliedArtefacts artefacts = SuppliedArtefacts.ofViews(Map.of(PATIENTS_URL, supplied));

      final Optional<SuppliedArtefact> matched = artefacts.match(PATIENTS_URL, null);

      assertThat(matched).isPresent();
      assertThat(matched.get().getView()).isSameAs(supplied);
      assertThat(matched.get().getUrl()).isEqualTo(PATIENTS_URL);
    }

    @Test
    void matchesNoSuppliedViewWhenNoneCarriesTheUrl() {
      assertThat(SuppliedArtefacts.empty().match(PATIENTS_URL, null)).isEmpty();
    }
  }

  // ---------------------------------------------------------------------------
  // Url-less supplied-artefact rejection (ContextArtefactParser).
  // ---------------------------------------------------------------------------

  @Nested
  class SuppliedViewValidation {

    @Test
    void rejectsASuppliedViewWithoutAUrl() {
      // A supplied view is matched to a dependency by its url, so one without a url can never
      // satisfy anything and is rejected rather than silently ignored.
      final FhirViewValidator viewValidator = mock(FhirViewValidator.class);
      final ContextArtefactParser parser = new ContextArtefactParser(viewValidator);

      assertThatThrownBy(() -> parser.parse(List.of(viewDefinitionWithoutUrl())))
          .isInstanceOf(InvalidRequestException.class)
          .hasMessageContaining("url");
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers.
  // ---------------------------------------------------------------------------

  @Nonnull
  private static ViewDefinitionResource viewDefinitionWithoutUrl() {
    final ViewDefinitionResource view = new ViewDefinitionResource();
    view.setId("no-url-view");
    view.setName(new StringType("no_url_view"));
    view.setResource(new CodeType("Patient"));
    view.setStatus(new CodeType("active"));
    final SelectComponent select = new SelectComponent();
    final ColumnComponent column = new ColumnComponent();
    column.setName(new StringType("id"));
    column.setPath(new StringType("id"));
    select.getColumn().add(column);
    view.getSelect().add(select);
    return view;
  }
}
