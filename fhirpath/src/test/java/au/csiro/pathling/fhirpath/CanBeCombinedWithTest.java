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

import au.csiro.pathling.fhirpath.annotations.NotImplemented;
import au.csiro.pathling.test.SpringBootUnitTest;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

/**
 * @author John Grimes
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
@NotImplemented
class CanBeCombinedWithTest {

  // TODO: implement with columns
  
  // final ResourceCollection resourceCollection;
  // final UntypedResourcePath untypedResourcePath;
  //
  // final PrimitivePath booleanPath;
  // final PrimitivePath codingPath;
  // final PrimitivePath datePath;
  // final PrimitivePath dateTimePath;
  // final PrimitivePath decimalPath;
  // final PrimitivePath integerPath;
  // final ReferencePath referencePath;
  // final PrimitivePath stringPath;
  // final PrimitivePath timePath;
  //
  // final BooleanLiteralPath booleanLiteralPath;
  // final CodingLiteralPath codingLiteralPath;
  // final DateLiteralPath dateLiteralPath;
  // final DateTimeLiteralPath dateTimeLiteralPath;
  // final DecimalCollection decimalLiteralPath;
  // final IntegerLiteralPath integerLiteralPath;
  // final NullPath nullPath;
  // final StringLiteralPath stringLiteralPath;
  // final TimeLiteralPath timeLiteralPath;
  //
  // final Set<Collection> paths;
  //
  // @Autowired
  // CanBeCombinedWithTest(@Nonnull final SparkSession spark) throws ParseException {
  //   resourceCollection = new ResourcePathBuilder(spark)
  //       .build();
  //
  //   booleanPath = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.BOOLEAN)
  //       .build();
  //   codingPath = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.CODING)
  //       .build();
  //   datePath = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.DATE)
  //       .build();
  //   dateTimePath = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.DATETIME)
  //       .build();
  //   decimalPath = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.DECIMAL)
  //       .build();
  //   integerPath = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.INTEGER)
  //       .build();
  //   referencePath = (ReferencePath) new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.REFERENCE)
  //       .build();
  //   untypedResourcePath = UntypedResourcePath.build(referencePath, referencePath.getExpression());
  //   stringPath = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.STRING)
  //       .build();
  //   timePath = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.TIME)
  //       .build();
  //
  //   booleanLiteralPath = BooleanLiteralPath.fromString("true", resourceCollection);
  //   codingLiteralPath = CodingCollection
  //       .fromLiteral("http://snomed.info/sct|27699000", resourceCollection);
  //   dateLiteralPath = DateCollection.fromLiteral("@1983-06-21", resourceCollection);
  //   dateTimeLiteralPath = DateTimeLiteralPath
  //       .fromString("@2015-02-08T13:28:17-05:00", resourceCollection);
  //   decimalLiteralPath = DecimalCollection.fromLiteral("9.5", resourceCollection);
  //   integerLiteralPath = IntegerLiteralPath.fromString("14", resourceCollection);
  //   nullPath = NullPath.build(resourceCollection);
  //   stringLiteralPath = StringCollection.fromLiteral("'foo'", resourceCollection);
  //   timeLiteralPath = TimeCollection.fromLiteral("T12:30", resourceCollection);
  //
  //   paths = new HashSet<>(Arrays.asList(
  //       resourceCollection,
  //       untypedResourcePath,
  //       booleanPath,
  //       codingPath,
  //       datePath,
  //       dateTimePath,
  //       decimalPath,
  //       integerPath,
  //       referencePath,
  //       stringPath,
  //       timePath,
  //       booleanLiteralPath,
  //       codingLiteralPath,
  //       dateLiteralPath,
  //       dateTimeLiteralPath,
  //       decimalLiteralPath,
  //       integerLiteralPath,
  //       nullPath,
  //       stringLiteralPath,
  //       timeLiteralPath
  //   ));
  // }
  //
  // @Value
  // @EqualsAndHashCode
  // static class TestParameters {
  //
  //   @Nonnull
  //   Collection source;
  //
  //   @Nonnull
  //   Collection target;
  //
  //   boolean expectation;
  //
  //   @Override
  //   public String toString() {
  //     return source.getClass().getSimpleName() + ", " + target.getClass().getSimpleName() + ": "
  //         + expectation;
  //   }
  //
  // }
  //
  // Stream<TestParameters> parameters() {
  //   return Stream.of(
  //       buildParameters(resourceCollection, Collections.singletonList(resourceCollection)),
  //       buildParameters(untypedResourcePath, Collections.singletonList(untypedResourcePath)),
  //       buildParameters(booleanPath, Arrays.asList(booleanPath, booleanLiteralPath)),
  //       buildParameters(codingPath, Arrays.asList(codingPath, codingLiteralPath)),
  //       buildParameters(datePath, Arrays.asList(datePath, dateLiteralPath)),
  //       buildParameters(dateTimePath, Arrays.asList(dateTimePath, dateTimeLiteralPath)),
  //       buildParameters(decimalPath, Arrays.asList(decimalPath, decimalLiteralPath)),
  //       buildParameters(integerPath, Arrays.asList(integerPath, integerLiteralPath)),
  //       buildParameters(referencePath, Collections.singletonList(referencePath)),
  //       buildParameters(stringPath, Arrays.asList(stringPath, stringLiteralPath)),
  //       buildParameters(timePath, Arrays.asList(timePath, timeLiteralPath)),
  //       buildParameters(booleanLiteralPath, Arrays.asList(booleanLiteralPath, booleanPath)),
  //       buildParameters(codingLiteralPath, Arrays.asList(codingLiteralPath, codingPath)),
  //       buildParameters(dateLiteralPath, Arrays.asList(dateLiteralPath, datePath)),
  //       buildParameters(dateTimeLiteralPath, Arrays.asList(dateTimeLiteralPath, dateTimePath)),
  //       buildParameters(decimalLiteralPath, Arrays.asList(decimalLiteralPath, decimalPath)),
  //       buildParameters(integerLiteralPath, Arrays.asList(integerLiteralPath, integerPath)),
  //       buildParameters(stringLiteralPath, Arrays.asList(stringLiteralPath, stringPath)),
  //       buildParameters(timeLiteralPath, Arrays.asList(timeLiteralPath, timePath)),
  //       buildParameters(nullPath, paths)
  //   ).flatMap(x -> x).distinct();
  // }
  //
  // Stream<TestParameters> buildParameters(@Nonnull final Collection source,
  //     @Nonnull final java.util.Collection<Collection> canBeCombined) {
  //   return paths.stream().map(path -> new TestParameters(source, path,
  //       canBeCombined.contains(path) || path == nullPath));
  // }
  //
  // @ParameterizedTest
  // @MethodSource("parameters")
  // void canBeCombined(@Nonnull final TestParameters parameters) {
  //   final boolean result = parameters.getSource().canBeCombinedWith(parameters.getTarget());
  //   assertEquals(parameters.isExpectation(), result);
  // }
  //
}
