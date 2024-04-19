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

package au.csiro.pathling.fhirpath;

import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.element.ReferencePath;
import au.csiro.pathling.fhirpath.literal.BooleanLiteralPath;
import au.csiro.pathling.fhirpath.literal.CodingLiteralPath;
import au.csiro.pathling.fhirpath.literal.DateLiteralPath;
import au.csiro.pathling.fhirpath.literal.DateTimeLiteralPath;
import au.csiro.pathling.fhirpath.literal.DecimalLiteralPath;
import au.csiro.pathling.fhirpath.literal.IntegerLiteralPath;
import au.csiro.pathling.fhirpath.literal.NullLiteralPath;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.fhirpath.literal.TimeLiteralPath;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ResourcePathBuilder;
import jakarta.annotation.Nonnull;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author John Grimes
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class CanBeCombinedWithTest {

  final ResourcePath resourcePath;
  final UntypedResourcePath untypedResourcePath;

  final ElementPath booleanPath;
  final ElementPath codingPath;
  final ElementPath datePath;
  final ElementPath dateTimePath;
  final ElementPath decimalPath;
  final ElementPath integerPath;
  final ReferencePath referencePath;
  final ElementPath stringPath;
  final ElementPath timePath;

  final BooleanLiteralPath booleanLiteralPath;
  final CodingLiteralPath codingLiteralPath;
  final DateLiteralPath dateLiteralPath;
  final DateTimeLiteralPath dateTimeLiteralPath;
  final DecimalLiteralPath decimalLiteralPath;
  final IntegerLiteralPath integerLiteralPath;
  final NullLiteralPath nullLiteralPath;
  final StringLiteralPath stringLiteralPath;
  final TimeLiteralPath timeLiteralPath;

  final Set<FhirPath> paths;

  @Autowired
  CanBeCombinedWithTest(@Nonnull final SparkSession spark) throws ParseException {
    resourcePath = new ResourcePathBuilder(spark)
        .build();

    booleanPath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.BOOLEAN)
        .build();
    codingPath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODING)
        .build();
    datePath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.DATE)
        .build();
    dateTimePath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.DATETIME)
        .build();
    decimalPath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.DECIMAL)
        .build();
    integerPath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.INTEGER)
        .build();
    referencePath = (ReferencePath) new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.REFERENCE)
        .build();
    untypedResourcePath = UntypedResourcePath.build(referencePath, referencePath.getExpression());
    stringPath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.STRING)
        .build();
    timePath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.TIME)
        .build();

    booleanLiteralPath = BooleanLiteralPath.fromString("true", resourcePath);
    codingLiteralPath = CodingLiteralPath
        .fromString("http://snomed.info/sct|27699000", resourcePath);
    dateLiteralPath = DateLiteralPath.fromString("@1983-06-21", resourcePath);
    dateTimeLiteralPath = DateTimeLiteralPath
        .fromString("@2015-02-08T13:28:17-05:00", resourcePath);
    decimalLiteralPath = DecimalLiteralPath.fromString("9.5", resourcePath);
    integerLiteralPath = IntegerLiteralPath.fromString("14", resourcePath);
    nullLiteralPath = NullLiteralPath.build(resourcePath);
    stringLiteralPath = StringLiteralPath.fromString("'foo'", resourcePath);
    timeLiteralPath = TimeLiteralPath.fromString("T12:30", resourcePath);

    paths = new HashSet<>(Arrays.asList(
        resourcePath,
        untypedResourcePath,
        booleanPath,
        codingPath,
        datePath,
        dateTimePath,
        decimalPath,
        integerPath,
        referencePath,
        stringPath,
        timePath,
        booleanLiteralPath,
        codingLiteralPath,
        dateLiteralPath,
        dateTimeLiteralPath,
        decimalLiteralPath,
        integerLiteralPath,
        nullLiteralPath,
        stringLiteralPath,
        timeLiteralPath
    ));
  }

  @Value
  @EqualsAndHashCode
  static class TestParameters {

    @Nonnull
    FhirPath source;

    @Nonnull
    FhirPath target;

    boolean expectation;

    @Override
    public String toString() {
      return source.getClass().getSimpleName() + ", " + target.getClass().getSimpleName() + ": "
          + expectation;
    }

  }

  Stream<TestParameters> parameters() {
    return Stream.of(
        buildParameters(resourcePath, Collections.singletonList(resourcePath)),
        buildParameters(untypedResourcePath, Collections.singletonList(untypedResourcePath)),
        buildParameters(booleanPath, Arrays.asList(booleanPath, booleanLiteralPath)),
        buildParameters(codingPath, Arrays.asList(codingPath, codingLiteralPath)),
        buildParameters(datePath, Arrays.asList(datePath, dateLiteralPath)),
        buildParameters(dateTimePath, Arrays.asList(dateTimePath, dateTimeLiteralPath)),
        buildParameters(decimalPath, Arrays.asList(decimalPath, decimalLiteralPath)),
        buildParameters(integerPath, Arrays.asList(integerPath, integerLiteralPath)),
        buildParameters(referencePath, Collections.singletonList(referencePath)),
        buildParameters(stringPath, Arrays.asList(stringPath, stringLiteralPath)),
        buildParameters(timePath, Arrays.asList(timePath, timeLiteralPath)),
        buildParameters(booleanLiteralPath, Arrays.asList(booleanLiteralPath, booleanPath)),
        buildParameters(codingLiteralPath, Arrays.asList(codingLiteralPath, codingPath)),
        buildParameters(dateLiteralPath, Arrays.asList(dateLiteralPath, datePath)),
        buildParameters(dateTimeLiteralPath, Arrays.asList(dateTimeLiteralPath, dateTimePath)),
        buildParameters(decimalLiteralPath, Arrays.asList(decimalLiteralPath, decimalPath)),
        buildParameters(integerLiteralPath, Arrays.asList(integerLiteralPath, integerPath)),
        buildParameters(stringLiteralPath, Arrays.asList(stringLiteralPath, stringPath)),
        buildParameters(timeLiteralPath, Arrays.asList(timeLiteralPath, timePath)),
        buildParameters(nullLiteralPath, paths)
    ).flatMap(x -> x).distinct();
  }

  Stream<TestParameters> buildParameters(@Nonnull final FhirPath source,
      @Nonnull final Collection<FhirPath> canBeCombined) {
    return paths.stream().map(path -> new TestParameters(source, path,
        canBeCombined.contains(path) || path == nullLiteralPath));
  }

  @ParameterizedTest
  @MethodSource("parameters")
  void canBeCombined(@Nonnull final TestParameters parameters) {
    final boolean result = parameters.getSource().canBeCombinedWith(parameters.getTarget());
    assertEquals(parameters.isExpectation(), result);
  }

}
