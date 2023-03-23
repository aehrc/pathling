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

package au.csiro.pathling.library.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import au.csiro.pathling.extract.ExtractRequest;
import au.csiro.pathling.query.ExpressionWithLabel;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

public class ExtractQueryTest {

  private final QueryExecutor mockSource = mock(QueryExecutor.class);

  @Captor
  private final ArgumentCaptor<ExtractRequest> requestCaptor = ArgumentCaptor.forClass(
      ExtractRequest.class);

  void testEmptyQuery() {
    final ExtractQuery query = ExtractQuery.of(ResourceType.PATIENT);
    query.execute(mockSource);
    verify(mockSource).execute(requestCaptor.capture());
    assertEquals(
        new ExtractRequest(ResourceType.PATIENT, Collections.emptyList(), Collections.emptyList(),
            Optional.empty()), requestCaptor.getValue());
  }

  @Test
  void testQueryWithNoFilters() {
    final ExtractQuery query = ExtractQuery.of(ResourceType.PATIENT)
        .withColumn("name")
        .withColumn("foo.bar()", "fooBar");

    query.execute(mockSource);
    verify(mockSource).execute(requestCaptor.capture());
    assertEquals(
        new ExtractRequest(ResourceType.PATIENT,
            List.of(
                ExpressionWithLabel.of("name", "name"),
                ExpressionWithLabel.of("foo.bar()", "fooBar")
            ),
            Collections.emptyList(),
            Optional.empty()),
        requestCaptor.getValue());
  }

  @Test
  void testQueryWithFilters() {
    final ExtractQuery query = ExtractQuery.of(ResourceType.CONDITION)
        .withColumn("foo.bar()", "fooBar")
        .withColumn("name")
        .withFilter("filter1")
        .withFilter("filter2");

    query.execute(mockSource);
    verify(mockSource).execute(requestCaptor.capture());
    assertEquals(
        new ExtractRequest(ResourceType.CONDITION,
            List.of(
                ExpressionWithLabel.of("foo.bar()", "fooBar"),
                ExpressionWithLabel.of("name", "name")
            ),
            List.of("filter1", "filter2"),
            Optional.empty()),
        requestCaptor.getValue());
  }
}
