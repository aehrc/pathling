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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.view.ViewExecutionHelper;
import au.csiro.pathling.operations.view.ViewExportFormat;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import java.util.Map;
import java.util.Set;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Reference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SqlQueryExportRequestParser}, verifying source rejection, strict format
 * parsing, query-source exclusivity, the no-query rejection, and the default format. The pipeline
 * and resolver collaborators are mocked.
 *
 * @author John Grimes
 */
class SqlQueryExportRequestParserTest {

  private SqlQueryPipeline pipeline;
  private LibraryReferenceResolver libraryReferenceResolver;
  private SqlQueryExportRequestParser parser;

  @BeforeEach
  void setUp() {
    pipeline = mock(SqlQueryPipeline.class);
    libraryReferenceResolver = mock(LibraryReferenceResolver.class);
    final ViewExecutionHelper viewExecutionHelper = mock(ViewExecutionHelper.class);
    final ServerConfiguration serverConfiguration = mock(ServerConfiguration.class);
    final QueryableDataSource deltaLake = mock(QueryableDataSource.class);
    parser =
        new SqlQueryExportRequestParser(
            pipeline,
            libraryReferenceResolver,
            viewExecutionHelper,
            FhirContext.forR4Cached(),
            serverConfiguration,
            deltaLake);

    final ParsedSqlQuery parsedQuery =
        new ParsedSqlQuery("SELECT id FROM patients", java.util.List.of(), java.util.List.of());
    final SqlQueryRequest request =
        new SqlQueryRequest(parsedQuery, SqlQueryOutputFormat.NDJSON, true, null, Map.of());
    when(pipeline.prepare(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(new PreparedSqlQuery(request, Map.of()));
    when(libraryReferenceResolver.resolve(any())).thenReturn(new Library());
  }

  @Test
  void parsesSingleStoredQueryWithDefaultNdjsonFormat() {
    final Parameters body = parameters(queryWithReference("Library/patient-query"));

    final SqlQueryExportRequest request =
        parser.parse(requestDetails(body), null, null, null, null, Set.of(), null, null);

    assertThat(request.queries()).hasSize(1);
    assertThat(request.format()).isEqualTo(ViewExportFormat.NDJSON);
  }

  @Test
  void rejectsBothQuerySourcesWith400() {
    final Parameters.ParametersParameterComponent query =
        queryWithReference("Library/patient-query");
    query.addPart().setName("queryResource").setResource(new Library());
    final Parameters body = parameters(query);

    assertThatThrownBy(
            () -> parser.parse(requestDetails(body), null, null, null, null, Set.of(), null, null))
        .isInstanceOf(InvalidRequestException.class);
  }

  @Test
  void rejectsNeitherQuerySourceWith400() {
    final Parameters.ParametersParameterComponent query = new Parameters().addParameter();
    query.setName("query");
    query.addPart().setName("name").setValue(new org.hl7.fhir.r4.model.StringType("orphan"));
    final Parameters body = parameters(query);

    assertThatThrownBy(
            () -> parser.parse(requestDetails(body), null, null, null, null, Set.of(), null, null))
        .isInstanceOf(InvalidRequestException.class);
  }

  @Test
  void rejectsNoQueryAtSystemLevelWith400() {
    assertThatThrownBy(
            () ->
                parser.parse(
                    requestDetails(new Parameters()), null, null, null, null, Set.of(), null, null))
        .isInstanceOf(InvalidRequestException.class);
  }

  @Test
  void rejectsSourceParameterWith400BeforeResolvingAnyQuery() {
    final Parameters body = parameters(queryWithReference("Library/patient-query"));

    assertThatThrownBy(
            () ->
                parser.parse(
                    requestDetails(body), null, null, null, null, Set.of(), null, "s3://bucket"))
        .isInstanceOf(InvalidRequestException.class);
    verifyNoInteractions(pipeline, libraryReferenceResolver);
  }

  @Test
  void rejectsUnsupportedFormatWith400() {
    final Parameters body = parameters(queryWithReference("Library/patient-query"));

    assertThatThrownBy(
            () ->
                parser.parse(requestDetails(body), null, "json", null, null, Set.of(), null, null))
        .isInstanceOf(InvalidRequestException.class);
  }

  @Test
  void missingQueryReferenceLibraryPropagatesNotFound() {
    when(libraryReferenceResolver.resolve(any()))
        .thenThrow(new ResourceNotFoundException("Library with ID 'missing' not found"));
    final Parameters body = parameters(queryWithReference("Library/missing"));

    assertThatThrownBy(
            () -> parser.parse(requestDetails(body), null, null, null, null, Set.of(), null, null))
        .isInstanceOf(ResourceNotFoundException.class);
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private static Parameters.ParametersParameterComponent queryWithReference(final String ref) {
    final Parameters.ParametersParameterComponent query =
        new Parameters.ParametersParameterComponent().setName("query");
    query.addPart().setName("queryReference").setValue(new Reference(ref));
    return query;
  }

  private static Parameters parameters(final Parameters.ParametersParameterComponent... params) {
    final Parameters parameters = new Parameters();
    for (final Parameters.ParametersParameterComponent param : params) {
      parameters.addParameter(param);
    }
    return parameters;
  }

  private static ServletRequestDetails requestDetails(final Parameters body) {
    final ServletRequestDetails requestDetails = mock(ServletRequestDetails.class);
    when(requestDetails.getResource()).thenReturn(body);
    when(requestDetails.getCompleteUrl()).thenReturn("http://localhost/fhir/$sqlquery-export");
    when(requestDetails.getFhirServerBase()).thenReturn("http://localhost/fhir");
    return requestDetails;
  }
}
