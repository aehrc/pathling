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

package au.csiro.pathling.aggregate;

import jakarta.annotation.Nonnull;
import java.util.stream.Stream;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * @author John Grimes
 */
@Slf4j
class GroupingLiteralTest extends AggregateExecutorTest {

  static Stream<TestParameters> parameters() {
    return Stream
        .of(new TestParameters("BooleanLiteral", "true"),
            new TestParameters("StringLiteral", "'foo'"),
            new TestParameters("IntegerLiteral", "2"),
            new TestParameters("DecimalLiteral", "2.4"),
            new TestParameters("DateLiteral", "@2013-06-10"),
            new TestParameters("DateTimeLiteral", "@2015-02-08T13:28:17-05:00"),
            new TestParameters("TimeLiteral", "@T12:54"),
            new TestParameters("CodingLiteral", "http://somecodesystem.org|ABC"));
  }

  @Value
  static class TestParameters {

    @Nonnull
    String name;

    @Nonnull
    String fhirPath;

    @Override
    public String toString() {
      return name;
    }

  }

  @ParameterizedTest
  @MethodSource("parameters")
  void queryWithLiteralGroupingExpression(final TestParameters parameters) {
    subjectResource = ResourceType.MEDICATIONREQUEST;
    mockResource(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count()")
        .withGrouping(parameters.getFhirPath())
        .build();

    response = executor.execute(request);
    assertResponse("GroupingLiteralTest/" + parameters + ".Parameters.json", response);
  }

}
