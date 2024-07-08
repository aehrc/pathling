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

package au.csiro.pathling.fhirpath.function;

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
class UntilFunctionTest {

  // TODO: implement with columns
  //
  //
  // @Autowired
  // SparkSession spark;
  //
  // @Autowired
  // FhirContext fhirContext;
  //
  // static final String ID_ALIAS = "_abc123";
  // static final String[] IDS = {"patient-1", "patient-2", "patient-3", "patient-4", "patient-5",
  //     "patient-6"};
  //
  // Dataset<Row> leftDataset() {
  //   return new DatasetBuilder(spark)
  //       .withIdColumn(ID_ALIAS)
  //       .withColumn(DataTypes.StringType)
  //       .withRow("patient-1", "2020-01-01T00:00:00Z")
  //       .withRow("patient-2", "2020-01-01T00:00:00Z")
  //       .withRow("patient-3", "2020-01-01")
  //       .withRow("patient-4", null)
  //       .withRow("patient-5", "2020-01-01T00:00:00Z")
  //       .withRow("patient-6", null)
  //       .build();
  // }
  //
  // Dataset<Row> rightDataset() {
  //   return new DatasetBuilder(spark)
  //       .withIdColumn(ID_ALIAS)
  //       .withColumn(DataTypes.StringType)
  //       .withRow("patient-1", "2021-01-01T00:00:00Z")
  //       .withRow("patient-2", "2021-01-01")
  //       .withRow("patient-3", "2021-01-01T00:00:00Z")
  //       .withRow("patient-4", "2021-01-01T00:00:00Z")
  //       .withRow("patient-5", null)
  //       .withRow("patient-6", null)
  //       .build();
  // }
  //
  // static final Object[] YEARS_RESULT = {1, 1, 1, null, null, null};
  // static final Object[] MONTHS_RESULT = {12, 12, 12, null, null, null};
  // static final Object[] DAYS_RESULT = {366, 366, 366, null, null, null};
  // static final Object[] HOURS_RESULT = {8784, 8784, 8784, null, null, null};
  // static final Object[] MINUTES_RESULT = {527040, 527040, 527040, null, null, null};
  // static final Object[] SECONDS_RESULT = {31622400, 31622400, 31622400, null, null, null};
  // private static final ImmutableMap<String, Object[]> CALENDAR_DURATION_TO_RESULT = new Builder<String, Object[]>()
  //     .put("years", YEARS_RESULT)
  //     .put("months", MONTHS_RESULT)
  //     .put("days", DAYS_RESULT)
  //     .put("hours", HOURS_RESULT)
  //     .put("minutes", MINUTES_RESULT)
  //     .put("seconds", SECONDS_RESULT)
  //     .put("year", YEARS_RESULT)
  //     .put("month", MONTHS_RESULT)
  //     .put("day", DAYS_RESULT)
  //     .put("hour", HOURS_RESULT)
  //     .put("minute", MINUTES_RESULT)
  //     .put("second", SECONDS_RESULT)
  //     .build();
  //
  // @Value
  // static class TestParameters {
  //
  //   @Nonnull
  //   String name;
  //
  //   @Nonnull
  //   NonLiteralPath input;
  //
  //   @Nonnull
  //   List<Collection> arguments;
  //
  //   @Nonnull
  //   ParserContext context;
  //
  //   @Nonnull
  //   Dataset<Row> expectedResult;
  //
  //   @Override
  //   public String toString() {
  //     return name;
  //   }
  //
  // }
  //
  // @Nonnull
  // Stream<TestParameters> parameters() {
  //   final java.util.Collection<TestParameters> parameters = new ArrayList<>();
  //   for (final String calendarDuration : CALENDAR_DURATION_TO_RESULT.keySet()) {
  //     final PrimitivePath input = new ElementPathBuilder(spark)
  //         .fhirType(FHIRDefinedType.DATETIME)
  //         .dataset(leftDataset())
  //         .idAndValueColumns()
  //         .singular(true)
  //         .build();
  //     final PrimitivePath argument = new ElementPathBuilder(spark)
  //         .fhirType(FHIRDefinedType.DATETIME)
  //         .dataset(rightDataset())
  //         .idAndValueColumns()
  //         .singular(true)
  //         .build();
  //     final ParserContext context = new ParserContextBuilder(spark, fhirContext)
  //         .groupingColumns(Collections.singletonList(input.getIdColumn()))
  //         .build();
  //     final Dataset<Row> expectedResult = new DatasetBuilder(spark)
  //         .withIdColumn(ID_ALIAS)
  //         .withColumn(DataTypes.IntegerType)
  //         .withIdValueRows(Arrays.asList(IDS), id -> {
  //           final int index = Integer.parseInt(id.split("-")[1]) - 1;
  //           final Object[] results = CALENDAR_DURATION_TO_RESULT.get(calendarDuration);
  //           assertNotNull(results);
  //           return results[index];
  //         }).build();
  //     final List<Collection> arguments = List.of(argument,
  //         StringCollection.fromLiteral(calendarDuration, input));
  //     parameters.add(
  //         new TestParameters(calendarDuration, input, arguments, context, expectedResult));
  //   }
  //   return parameters.stream();
  // }
  //
  // @ParameterizedTest
  // @MethodSource("parameters")
  // void test(@Nonnull final TestParameters parameters) {
  //   final NamedFunctionInput input = new NamedFunctionInput(parameters.getContext(),
  //       parameters.getInput(), parameters.getArguments());
  //   final Collection result = NamedFunction.getInstance("until").invoke(input);
  //   assertThat(result)
  //       .isElementPath(IntegerCollection.class)
  //       .selectResult()
  //       .hasRows(parameters.getExpectedResult());
  // }
  //
  // @Test
  // void milliseconds() {
  //   final Dataset<Row> leftDataset = new DatasetBuilder(spark)
  //       .withIdColumn(ID_ALIAS)
  //       .withColumn(DataTypes.StringType)
  //       .withRow("patient-1", "2020-01-01")
  //       .build();
  //   final Dataset<Row> rightDataset = new DatasetBuilder(spark)
  //       .withIdColumn(ID_ALIAS)
  //       .withColumn(DataTypes.StringType)
  //       .withRow("patient-1", "2020-01-02")
  //       .build();
  //   final PrimitivePath input = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.DATE)
  //       .dataset(leftDataset)
  //       .idAndValueColumns()
  //       .singular(true)
  //       .build();
  //   final PrimitivePath argument = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.DATE)
  //       .dataset(rightDataset)
  //       .idAndValueColumns()
  //       .singular(true)
  //       .build();
  //   final ParserContext context = new ParserContextBuilder(spark, fhirContext)
  //       .groupingColumns(Collections.singletonList(input.getIdColumn()))
  //       .build();
  //   final Dataset<Row> expectedResult = new DatasetBuilder(spark)
  //       .withIdColumn(ID_ALIAS)
  //       .withColumn(DataTypes.IntegerType)
  //       .withRow("patient-1", 86400000)
  //       .build();
  //   final List<Collection> arguments1 = List.of(argument,
  //       StringCollection.fromLiteral("millisecond", input));
  //   final List<Collection> arguments2 = List.of(argument,
  //       StringCollection.fromLiteral("millisecond", input));
  //   for (final List<Collection> arguments : List.of(arguments1, arguments2)) {
  //     final NamedFunctionInput functionInput = new NamedFunctionInput(context, input, arguments);
  //     final Collection result = NamedFunction.getInstance("until").invoke(functionInput);
  //     assertThat(result)
  //         .isElementPath(IntegerCollection.class)
  //         .selectResult()
  //         .hasRows(expectedResult);
  //   }
  // }
  //
  // @Test
  // void dateLiteralArgument() throws ParseException {
  //   final Dataset<Row> leftDataset = new DatasetBuilder(spark)
  //       .withIdColumn(ID_ALIAS)
  //       .withColumn(DataTypes.StringType)
  //       .withRow("patient-1", "2020-01-01")
  //       .build();
  //   final PrimitivePath input = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.DATE)
  //       .dataset(leftDataset)
  //       .idAndValueColumns()
  //       .singular(true)
  //       .build();
  //   final DateLiteralPath argument = DateCollection.fromLiteral("2020-01-02", input);
  //   final ParserContext context = new ParserContextBuilder(spark, fhirContext)
  //       .groupingColumns(Collections.singletonList(input.getIdColumn()))
  //       .build();
  //   final Dataset<Row> expectedResult = new DatasetBuilder(spark)
  //       .withIdColumn(ID_ALIAS)
  //       .withColumn(DataTypes.IntegerType)
  //       .withRow("patient-1", 86400000)
  //       .build();
  //   final List<Collection> arguments = List.of(argument,
  //       StringCollection.fromLiteral("millisecond", input));
  //   final NamedFunctionInput functionInput = new NamedFunctionInput(context, input, arguments);
  //   final Collection result = NamedFunction.getInstance("until").invoke(functionInput);
  //   assertThat(result)
  //       .isElementPath(IntegerCollection.class)
  //       .selectResult()
  //       .hasRows(expectedResult);
  // }
  //
  // @Test
  // void dateTimeLiteralArgument() throws ParseException {
  //   final Dataset<Row> leftDataset = new DatasetBuilder(spark)
  //       .withIdColumn(ID_ALIAS)
  //       .withColumn(DataTypes.StringType)
  //       .withRow("patient-1", "2020-01-01")
  //       .build();
  //   final PrimitivePath input = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.DATE)
  //       .dataset(leftDataset)
  //       .idAndValueColumns()
  //       .singular(true)
  //       .build();
  //   final DateTimeLiteralPath argument = DateTimeLiteralPath.fromString("2020-01-02T00:00:00Z",
  //       input);
  //   final ParserContext context = new ParserContextBuilder(spark, fhirContext)
  //       .groupingColumns(Collections.singletonList(input.getIdColumn()))
  //       .build();
  //   final Dataset<Row> expectedResult = new DatasetBuilder(spark)
  //       .withIdColumn(ID_ALIAS)
  //       .withColumn(DataTypes.IntegerType)
  //       .withRow("patient-1", 86400000)
  //       .build();
  //   final List<Collection> arguments = List.of(argument,
  //       StringCollection.fromLiteral("millisecond", input));
  //   final NamedFunctionInput functionInput = new NamedFunctionInput(context, input, arguments);
  //   final Collection result = NamedFunction.getInstance("until").invoke(functionInput);
  //   assertThat(result)
  //       .isElementPath(IntegerCollection.class)
  //       .selectResult()
  //       .hasRows(expectedResult);
  // }
  //
  // @Test
  // void yearOnlyDateInput() throws ParseException {
  //   final Dataset<Row> leftDataset = new DatasetBuilder(spark)
  //       .withIdColumn(ID_ALIAS)
  //       .withColumn(DataTypes.StringType)
  //       .withRow("patient-1", "2020")
  //       .build();
  //   final ElementPath input = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.DATE)
  //       .dataset(leftDataset)
  //       .idAndValueColumns()
  //       .singular(true)
  //       .build();
  //   final DateTimeLiteralPath argument = DateTimeLiteralPath.fromString("2020-01-02T00:00:00Z",
  //       input);
  //   final ParserContext context = new ParserContextBuilder(spark, fhirContext)
  //       .groupingColumns(Collections.singletonList(input.getIdColumn()))
  //       .build();
  //   final Dataset<Row> expectedResult = new DatasetBuilder(spark)
  //       .withIdColumn(ID_ALIAS)
  //       .withColumn(DataTypes.IntegerType)
  //       .withRow("patient-1", 86400000)
  //       .build();
  //   final List<FhirPath> arguments = List.of(argument,
  //       StringLiteralPath.fromString("millisecond", input));
  //   final NamedFunctionInput functionInput = new NamedFunctionInput(context, input, arguments);
  //   final FhirPath result = NamedFunction.getInstance("until").invoke(functionInput);
  //   assertThat(result)
  //       .isElementPath(IntegerPath.class)
  //       .selectResult()
  //       .hasRows(expectedResult);
  // }
  //
  // @Test
  // void invalidCalendarDuration() {
  //   final Dataset<Row> leftDataset = new DatasetBuilder(spark)
  //       .withIdColumn(ID_ALIAS)
  //       .withColumn(DataTypes.StringType)
  //       .withRow("patient-1", "2020-01-01")
  //       .build();
  //   final PrimitivePath input = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.DATE)
  //       .dataset(leftDataset)
  //       .idAndValueColumns()
  //       .singular(true)
  //       .build();
  //
  //   final DateTimeCollection argument = mock(DateTimeCollection.class);
  //   when(argument.isSingular()).thenReturn(true);
  //
  //   final NamedFunctionInput functionInput = new NamedFunctionInput(mock(ParserContext.class),
  //       input,
  //       List.of(argument, StringCollection.fromLiteral("nanosecond", input)));
  //   final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
  //       () -> NamedFunction.getInstance("until").invoke(functionInput));
  //   assertEquals("Invalid calendar duration: nanosecond", error.getMessage());
  // }
  //
  // @Test
  // void wrongNumberOfArguments() {
  //   final NamedFunctionInput input = new NamedFunctionInput(mock(ParserContext.class),
  //       mock(DateTimeCollection.class), List.of(mock(DateTimeCollection.class)));
  //   final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
  //       () -> NamedFunction.getInstance("until").invoke(input));
  //   assertEquals("until function must have two arguments", error.getMessage());
  // }
  //
  // @Test
  // void wrongInputType() {
  //   final NamedFunctionInput input = new NamedFunctionInput(mock(ParserContext.class),
  //       mock(StringCollection.class),
  //       List.of(mock(DateTimeCollection.class), mock(StringLiteralPath.class)));
  //   final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
  //       () -> NamedFunction.getInstance("until").invoke(input));
  //   assertEquals("until function must be invoked on a DateTime or Date", error.getMessage());
  // }
  //
  // @Test
  // void wrongArgumentType() {
  //   final NamedFunctionInput input = new NamedFunctionInput(mock(ParserContext.class),
  //       mock(DateCollection.class),
  //       List.of(mock(ReferencePath.class), mock(StringLiteralPath.class)));
  //   final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
  //       () -> NamedFunction.getInstance("until").invoke(input));
  //   assertEquals("until function must have a DateTime or Date as the first argument",
  //       error.getMessage());
  // }
  //
  // @Test
  // void inputNotSingular() {
  //   final DateTimeCollection input = mock(DateTimeCollection.class);
  //   final NamedFunctionInput functionInput = new NamedFunctionInput(mock(ParserContext.class),
  //       input, List.of(mock(DateTimeLiteralPath.class),
  //       StringCollection.fromLiteral("nanosecond", input)));
  //   final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
  //       () -> NamedFunction.getInstance("until").invoke(functionInput));
  //   assertEquals("until function must be invoked on a singular path", error.getMessage());
  // }
  //
  // @Test
  // void argumentNotSingular() {
  //   final DateTimeCollection input = mock(DateTimeCollection.class);
  //   when(input.isSingular()).thenReturn(true);
  //   final NamedFunctionInput functionInput = new NamedFunctionInput(mock(ParserContext.class),
  //       input,
  //       List.of(mock(DateTimeLiteralPath.class),
  //           StringCollection.fromLiteral("nanosecond", input)));
  //   final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
  //       () -> NamedFunction.getInstance("until").invoke(functionInput));
  //   assertEquals("until function must have the singular path as its first argument",
  //       error.getMessage());
  // }
}
