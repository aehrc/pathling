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

import au.csiro.pathling.aggregate.AggregateRequest;
import au.csiro.pathling.query.ExpressionWithLabel;
import java.util.Collections;
import java.util.List;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

public class AggregateQueryTest {

  private final QueryExecutor mockSource = mock(QueryExecutor.class);

  @Captor
  private final ArgumentCaptor<AggregateRequest> requestCaptor = ArgumentCaptor.forClass(
      AggregateRequest.class);

  @Test
  void testEmptyQuery() {
    final AggregateQuery query = AggregateQuery.of(ResourceType.PATIENT);
    query.execute(mockSource);
    verify(mockSource).execute(requestCaptor.capture());
    assertEquals(
        new AggregateRequest(ResourceType.PATIENT, Collections.emptyList(), Collections.emptyList(),
            Collections.emptyList()), requestCaptor.getValue());
  }

  @Test
  void testQueryWithAggregationsOnly() {
    final AggregateQuery query = AggregateQuery.of(ResourceType.PATIENT)
        .withAggregation("exp.agg1")
        .withAggregation("exp.agg2", "agg2");

    query.execute(mockSource);
    verify(mockSource).execute(requestCaptor.capture());
    verify(mockSource).execute(requestCaptor.capture());
    assertEquals(
        new AggregateRequest(ResourceType.PATIENT,
            List.of(
                ExpressionWithLabel.of("exp.agg1", "exp.agg1"),
                ExpressionWithLabel.of("exp.agg2", "agg2")
            ),
            Collections.emptyList(),
            Collections.emptyList()),
        requestCaptor.getValue());
  }


  @Test
  void testQueryWithAggregationsAndGroupings() {
    final AggregateQuery query = AggregateQuery.of(ResourceType.CONDITION)
        .withAggregation("exp.agg2", "agg2")
        .withAggregation("exp.agg1")
        .withGrouping("exp.group1")
        .withGrouping("exp.group2", "group2");

    query.execute(mockSource);
    verify(mockSource).execute(requestCaptor.capture());
    verify(mockSource).execute(requestCaptor.capture());
    assertEquals(
        new AggregateRequest(ResourceType.CONDITION,
            List.of(
                ExpressionWithLabel.of("exp.agg2", "agg2"),
                ExpressionWithLabel.of("exp.agg1", "exp.agg1")
            ),
            List.of(
                ExpressionWithLabel.of("exp.group1", "exp.group1"),
                ExpressionWithLabel.of("exp.group2", "group2")
            ),
            Collections.emptyList()),
        requestCaptor.getValue());
  }

  @Test
  void testQueryWithAggregationsAndGroupingsAndFilters() {
    final AggregateQuery query = AggregateQuery.of(ResourceType.OBSERVATION)
        .withAggregation("exp.agg1")
        .withAggregation("exp.agg2", "agg2")
        .withGrouping("exp.group2", "group2")
        .withGrouping("exp.group1")
        .withFilter("exp.filter1")
        .withFilter("exp.filter2");

    query.execute(mockSource);
    verify(mockSource).execute(requestCaptor.capture());
    assertEquals(
        new AggregateRequest(ResourceType.OBSERVATION,
            List.of(
                ExpressionWithLabel.of("exp.agg1", "exp.agg1"),
                ExpressionWithLabel.of("exp.agg2", "agg2")
            ),
            List.of(
                ExpressionWithLabel.of("exp.group2", "group2"),
                ExpressionWithLabel.of("exp.group1", "exp.group1")
            ),
            List.of("exp.filter1", "exp.filter2")
        ),
        requestCaptor.getValue());
  }
}
