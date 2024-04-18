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

package au.csiro.pathling.fhirpath.execution;

import static java.util.Collections.emptyMap;
import static org.mockito.Mockito.mock;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.io.source.DataSource;
import ca.uhn.fhir.context.FhirContext;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Test;

class FhirPathExecutorTest {

  @Nonnull
  final DataSource dataSource = mock(DataSource.class);

  @Test
  void testValidateSimpleResourcePath() {

    final Parser parser = new Parser();
    final FhirPath path = parser.parse(
        "where(name.family='Smith').name.given.join(',')");
    System.out.println(path.toExpression());
    final FhirPathExecutor validator = new SingleFhirPathExecutor(
        ResourceType.PATIENT,
        FhirContext.forR4(),
        StaticFunctionRegistry.getInstance(),
        emptyMap(),
        dataSource);

    final Collection result = validator.validate(path);
    System.out.println(result);
  }

  @Test
  void testValidateReverseResolve() {

    final Parser parser = new Parser();
    final FhirPath path = parser.parse(
        "where(name.family='Smith').reverseResolve(Condition.subject).code.coding");
    System.out.println(path.toExpression());
    final FhirPathExecutor validator = new MultiFhirPathExecutor(
        ResourceType.PATIENT,
        FhirContext.forR4(),
        StaticFunctionRegistry.getInstance(),
        dataSource);

    final Collection result = validator.validate(path);
    System.out.println(result);
  }
}
