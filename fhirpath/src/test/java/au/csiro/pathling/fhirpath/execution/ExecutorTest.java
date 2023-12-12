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

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.test.SpringBootUnitTest;
import ca.uhn.fhir.context.FhirContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import javax.annotation.Nonnull;
import java.util.List;
import java.util.function.Function;

import static org.mockito.Mockito.when;

@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
public class ExecutorTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  @Autowired
  FhirEncoders fhirEncoders;


  @MockBean
  DataSource dataSource;


  static class Builder<T> {

    private final T value;

    Builder(final T value) {
      this.value = value;
    }

    public <R> Builder<T> with(final Function<T, R> modifier) {
      modifier.apply(value);
      return this;
    }

    public T build() {
      return value;
    }

    public static <T> Builder<T> of(@Nonnull final T value) {
      return new Builder<>(value);
    }
  }

  static final Patient patient_1 = Builder.of(new Patient())
      .with(p -> p.setId("patient_1"))
      .with(p -> p.setGender(AdministrativeGender.FEMALE))
      .with(p -> p
          .addName()
          .setFamily("Smith")
          .addGiven("Jane").addGiven("Ann"))
      .build();

  static final Patient patient_2 = Builder.of(new Patient())
      .with(p -> p.setId("patient_2"))
      .with(p -> p.setGender(AdministrativeGender.MALE))
      .with(p -> p
          .addName()
          .setFamily("Rudd")
          .addGiven("Paul").addGiven("Peter"))
      .build();

  @Test
  void test() {
    final Dataset<Row> patients = spark.createDataset(
        List.of(patient_1, patient_2), fhirEncoders.of(Patient.class)).toDF().cache();
    when(dataSource.read(ResourceType.PATIENT)).thenReturn(patients);
    patients.select("id", "gender", "name").show();
    final Parser parser = new Parser();
    final FhirPath path = parser.parse(
        "where(gender='female').name.where(family.where($this='Smith').exists()).given.join(',')");
    System.out.println(path.toExpression());

    final FhirPathExecutor executor = new FhirPathExecutor(
        FhirContext.forR4(),
        StaticFunctionRegistry.getInstance(),
        ResourceType.PATIENT);

    final Dataset<Row> result = executor.execute(path, dataSource);
    result.show();
    System.out.println(result.queryExecution().executedPlan().toString());
  }


  @Test
  void testReverseResolve() {
    final Dataset<Row> patients = spark.createDataset(
        List.of(patient_1, patient_2), fhirEncoders.of(Patient.class)).toDF().cache();
    when(dataSource.read(ResourceType.PATIENT)).thenReturn(patients);
    patients.select("id", "gender", "name").show();
    final Parser parser = new Parser();
    final FhirPath path = parser.parse(
        "reverseResolve(Condition.subject).count()");
    System.out.println(path.toExpression());

    final FhirPathExecutor executor = new FhirPathExecutor(
        FhirContext.forR4(),
        StaticFunctionRegistry.getInstance(),
        ResourceType.PATIENT);

    final Dataset<Row> result = executor.execute(path, dataSource);
    result.show();
    System.out.println(result.queryExecution().executedPlan().toString());
  }
}
