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

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.path.Paths.EvalFunction;
import ca.uhn.fhir.context.FhirContext;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Test;
import java.util.List;

class FhirPathExecutorTest {

  @Test
  void testValidateSimpleResourcePath() {

    final Parser parser = new Parser();
    final FhirPath<Collection> path = parser.parse(
        "where(name.family='Smith').name.given.join(',')");
    System.out.println(path.toExpression());
    final FhirPathExecutor validator = new FhirPathExecutor(
        FhirContext.forR4(),
        StaticFunctionRegistry.getInstance(),
        ResourceType.PATIENT);

    final Collection result = validator.validate(path);
    System.out.println(result);
  }

  @Test
  void testValidateReverseResolve() {

    final Parser parser = new Parser();
    final FhirPath<Collection> path = parser.parse(
        "where(name.family='Smith').reverseResolve(Condition.subject).code.coding");
    System.out.println(path.toExpression());
    final FhirPathExecutor validator = new FhirPathExecutor(
        FhirContext.forR4(),
        StaticFunctionRegistry.getInstance(),
        ResourceType.PATIENT);

    final Collection result = validator.validate(path);
    System.out.println(result);
  }


  @Test
  void testValidateResourceReference() {

    final Parser parser = new Parser();
    final FhirPath<Collection> path = parser.parse(
        "Patient.id");
    System.out.println(path.toExpression());
    final FhirPathExecutor validator = new FhirPathExecutor(
        FhirContext.forR4(),
        StaticFunctionRegistry.getInstance(),
        ResourceType.PATIENT);

    final Collection result = validator.validate(path);
    System.out.println(result);
  }



  @Test
  void testJoinPlanner() {
    final Parser parser = new Parser();
    final FhirPath<Collection> path = parser.parse(
        "reverseResolve(Condition.subject)");
    System.out.println(path.toExpression());
    final FhirPathExecutor validator = new FhirPathExecutor(
        FhirContext.forR4(),
        StaticFunctionRegistry.getInstance(),
        ResourceType.PATIENT);

    final List<EvalFunction> joins = validator.findJoins(
        path);
    
    System.out.println(joins);
    
  }
  
}
