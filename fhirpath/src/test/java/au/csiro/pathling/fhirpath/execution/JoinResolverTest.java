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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.execution.DataRoot.ReverseResolveRoot;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.io.source.DataSource;
import ca.uhn.fhir.context.FhirContext;
import java.util.Collections;
import java.util.Set;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Test;
import javax.annotation.Nonnull;

class JoinResolverTest {

  @Nonnull
  final DataSource dataSource = mock(DataSource.class);

  @Test
  void testSimpleJoin() {
    final Parser parser = new Parser();
    final FhirPath path = parser.parse(
        "reverseResolve(Condition.subject).id");
    System.out.println(path.toExpression());
    final FhirPathExecutor validator = new FhirPathExecutor(
        ResourceType.PATIENT,
        FhirContext.forR4(),
        StaticFunctionRegistry.getInstance(),
        dataSource);

    final Set<DataRoot> joins = validator.findJoinsRoots(path);
    System.out.println(joins);
    assertEquals(
        Set.of(
            ReverseResolveRoot.ofResource(ResourceType.PATIENT, ResourceType.CONDITION, "subject")),
        joins);
  }
  
  @Test
  void testSimpleJoinWithNoDependencies() {
    final Parser parser = new Parser();
    final FhirPath path = parser.parse(
        "reverseResolve(Condition.subject)");
    System.out.println(path.toExpression());
    final FhirPathExecutor validator = new FhirPathExecutor(
        ResourceType.PATIENT,
        FhirContext.forR4(),
        StaticFunctionRegistry.getInstance(),
        dataSource);

    final Set<DataRoot> joins = validator.findJoinsRoots(path);
    System.out.println(joins);
    assertEquals(
        Collections.emptySet(),
        joins);

  }

  @Test
  void testJoinInWhere() {
    final Parser parser = new Parser();
    final FhirPath path = parser.parse(
        "where(reverseResolve(Observation.subject).code.count() > 10).gender");
    System.out.println(path.toExpression());
    final FhirPathExecutor validator = new FhirPathExecutor(
        ResourceType.PATIENT,
        FhirContext.forR4(),
        StaticFunctionRegistry.getInstance(),
        dataSource);

    final Set<DataRoot> joins = validator.findJoinsRoots(
        path);
    System.out.println(joins);
    assertEquals(
        Set.of(ReverseResolveRoot.ofResource(ResourceType.PATIENT, ResourceType.OBSERVATION,
            "subject")),
        joins);

  }


  @Test
  void testMulitpleReverseResolveToTheSameJoin() {
    final Parser parser = new Parser();
    final FhirPath path = parser.parse(
        "where(reverseResolve(Condition.encounter).code.count() > 10).reverseResolve(Condition.encounter).code");
    System.out.println(path.toExpression());
    final FhirPathExecutor validator = new FhirPathExecutor(
        ResourceType.ENCOUNTER,
        FhirContext.forR4(),
        StaticFunctionRegistry.getInstance(),
        dataSource);

    final Set<DataRoot> joins = validator.findJoinsRoots(
        path);
    System.out.println(joins);
    assertEquals(
        Set.of(
            ReverseResolveRoot.ofResource(ResourceType.ENCOUNTER, ResourceType.CONDITION,
                "encounter")
        ),
        joins);
  }


  @Test
  void testJoinsInOperator() {
    final Parser parser = new Parser();
    final FhirPath path = parser.parse(
        "reverseResolve(Condition.subject).code.count() >  reverseResolve(Observation.subject).code.count()");
    System.out.println(path.toExpression());
    final FhirPathExecutor validator = new FhirPathExecutor(
        ResourceType.PATIENT,
        FhirContext.forR4(),
        StaticFunctionRegistry.getInstance(),
        dataSource);

    final Set<DataRoot> joins = validator.findJoinsRoots(
        path);
    System.out.println(joins);
    assertEquals(
        Set.of(
            ReverseResolveRoot.ofResource(ResourceType.PATIENT, ResourceType.OBSERVATION,
                "subject"),
            ReverseResolveRoot.ofResource(ResourceType.PATIENT, ResourceType.CONDITION, "subject")
        ),
        joins);
  }


  @Test
  void testChainedJoins() {
    final Parser parser = new Parser();
    final FhirPath path = parser.parse(
        "reverseResolve(Encounter.subject).reverseResolve(Condition.encounter).code.count()");
    System.out.println(path.toExpression());
    final FhirPathExecutor validator = new FhirPathExecutor(
        ResourceType.PATIENT,
        FhirContext.forR4(),
        StaticFunctionRegistry.getInstance(),
        dataSource);

    final Set<DataRoot> joins = validator.findJoinsRoots(
        path);
    System.out.println(joins);
    // TODO: not supported yet (reverse resolve on non master resoureces)
    // assertEquals(
    //     Set.of(
    //         ReverseResolveRoot.ofResource(ResourceType.PATIENT, ResourceType.ENCOUNTER, "subject"),
    //         ReverseResolveRoot.of(
    //             ReverseResolveRoot.ofResource(ResourceType.PATIENT, ResourceType.ENCOUNTER,
    //                 "subject"),
    //             ResourceType.CONDITION, "encounter")
    //     ),
    //     joins);
  }

}
