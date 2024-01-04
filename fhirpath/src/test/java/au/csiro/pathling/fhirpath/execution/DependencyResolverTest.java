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
import java.util.Set;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Test;

class DependencyResolverTest {


  @Nonnull
  final DataSource dataSource = mock(DataSource.class);

  @Test
  void testImplicitProperty() {
    final Parser parser = new Parser();
    final FhirPath path = parser.parse(
        "gender");
    System.out.println(path.toExpression());
    final MultiFhirpathExecutor validator = new MultiFhirpathExecutor(
        ResourceType.PATIENT,
        FhirContext.forR4(),
        StaticFunctionRegistry.getInstance(),
        dataSource);

    final Set<DataDependency> dependencies = validator.findDataDependencies(path);
    System.out.println(dependencies);

    assertEquals(Set.of(
        DataDependency.ofResource(ResourceType.PATIENT, "gender")
    ), dependencies);
  }

  @Test
  void testQualifiedProperty() {
    final Parser parser = new Parser();
    final FhirPath path = parser.parse(
        "Patient.gender");
    System.out.println(path.toExpression());
    final MultiFhirpathExecutor validator = new MultiFhirpathExecutor(
        ResourceType.PATIENT,
        FhirContext.forR4(),
        StaticFunctionRegistry.getInstance(),
        dataSource);

    final Set<DataDependency> dependencies = validator.findDataDependencies(path);
    System.out.println(dependencies);

    assertEquals(Set.of(
        DataDependency.ofResource(ResourceType.PATIENT, "gender")
    ), dependencies);
  }

  @Test
  void testMoreDependencies() {
    final Parser parser = new Parser();
    final FhirPath path = parser.parse(
        "Patient.name.family = Patient.gender");
    System.out.println(path.toExpression());
    final MultiFhirpathExecutor validator = new MultiFhirpathExecutor(
        ResourceType.PATIENT,
        FhirContext.forR4(),
        StaticFunctionRegistry.getInstance(),
        dataSource);

    final Set<DataDependency> dependencies = validator.findDataDependencies(path);
    System.out.println(dependencies);

    assertEquals(Set.of(
        DataDependency.ofResource(ResourceType.PATIENT, "name"),
        DataDependency.ofResource(ResourceType.PATIENT, "gender")
    ), dependencies);
  }

  @Test
  void testDependenciesAfterTransparentFunction() {
    final Parser parser = new Parser();
    final FhirPath path = parser.parse(
        "Patient.first().name.family = Condition.last().code");
    System.out.println(path.toExpression());
    final MultiFhirpathExecutor validator = new MultiFhirpathExecutor(
        ResourceType.PATIENT,
        FhirContext.forR4(),
        StaticFunctionRegistry.getInstance(),
        dataSource);

    final Set<DataDependency> dependencies = validator.findDataDependencies(path);
    System.out.println(dependencies);

    assertEquals(Set.of(
        DataDependency.ofResource(ResourceType.PATIENT, "name"),
        DataDependency.ofResource(ResourceType.CONDITION, "code")
    ), dependencies);
  }


  @Test
  void testInExpressions() {
    final Parser parser = new Parser();
    final FhirPath path = parser.parse(
        "Patient.where(name.family = first().gender).last().status");
    System.out.println(path.toExpression());
    final MultiFhirpathExecutor validator = new MultiFhirpathExecutor(
        ResourceType.PATIENT,
        FhirContext.forR4(),
        StaticFunctionRegistry.getInstance(),
        dataSource);

    final Set<DataDependency> dependencies = validator.findDataDependencies(path);
    System.out.println(dependencies);

    assertEquals(Set.of(
        DataDependency.ofResource(ResourceType.PATIENT, "name"),
        DataDependency.ofResource(ResourceType.PATIENT, "gender"),
        DataDependency.ofResource(ResourceType.PATIENT, "status")
    ), dependencies);
  }


  @Test
  void testFunctionArguments() {
    final Parser parser = new Parser();
    final FhirPath path = parser.parse(
        "where($this.where(identifier.count() >2).exists()).name.given.join(Patient.gender)");
    System.out.println(path.toExpression());
    final MultiFhirpathExecutor validator = new MultiFhirpathExecutor(
        ResourceType.PATIENT,
        FhirContext.forR4(),
        StaticFunctionRegistry.getInstance(),
        dataSource);

    final Set<DataDependency> dependencies = validator.findDataDependencies(path);
    System.out.println(dependencies);

    assertEquals(Set.of(
        DataDependency.ofResource(ResourceType.PATIENT, "name"),
        DataDependency.ofResource(ResourceType.PATIENT, "gender"),
        DataDependency.ofResource(ResourceType.PATIENT, "identifier")
    ), dependencies);
  }

  @Test
  void testReverseResolve() {
    final Parser parser = new Parser();
    final FhirPath path = parser.parse(
        "where(reverseResolve(Condition.subject).code.coding.count() > 20).name");
    System.out.println(path.toExpression());
    final MultiFhirpathExecutor validator = new MultiFhirpathExecutor(
        ResourceType.PATIENT,
        FhirContext.forR4(),
        StaticFunctionRegistry.getInstance(),
        dataSource);

    final Set<DataDependency> dependencies = validator.findDataDependencies(path);
    System.out.println(dependencies);

    assertEquals(Set.of(
        DataDependency.ofResource(ResourceType.PATIENT, "name"),
        DataDependency.of(
            ReverseResolveRoot.ofResource(ResourceType.PATIENT, ResourceType.CONDITION, "subject"),
            "code")
    ), dependencies);

    System.out.println(validator.findDataViews(path));
  }
}
