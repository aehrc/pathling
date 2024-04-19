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

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.literal.DateLiteralPath;
import au.csiro.pathling.fhirpath.literal.DateTimeLiteralPath;
import au.csiro.pathling.fhirpath.literal.QuantityLiteralPath;
import au.csiro.pathling.fhirpath.literal.TimeLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.text.ParseException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
public class DateArithmeticTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  static final String ID_ALIAS = "_abc123";

  @Value
  static class TestParameters {

    @Nonnull
    String name;

    @Nonnull
    FhirPath left;

    @Nonnull
    FhirPath right;

    @Nonnull
    ParserContext context;

    @Nonnull
    Operator operator;

    @Nonnull
    Dataset<Row> expectedResult;

    @Override
    public String toString() {
      return name;
    }

  }

  @Nonnull
  Stream<TestParameters> parameters() throws ParseException {
    final List<TestParameters> parameters = new ArrayList<>();

    final Dataset<Row> dateTimeDataset = new DatasetBuilder(spark)
        .withIdColumn(ID_ALIAS)
        .withColumn(DataTypes.StringType)
        .withRow("patient-1", "2015-02-07T13:28:17.000-05:00")
        .withRow("patient-2", "2017-01-01T00:00:00Z")
        .withRow("patient-3", "2025-06-21T00:15:00+10:00")
        .build();
    final ElementPath dateTimePath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.DATETIME)
        .dataset(dateTimeDataset)
        .idAndValueColumns()
        .singular(true)
        .build();

    final Dataset<Row> dateDataset = new DatasetBuilder(spark)
        .withIdColumn(ID_ALIAS)
        .withColumn(DataTypes.StringType)
        .withRow("patient-1", "2015-02-07")
        .withRow("patient-2", "2017-01-01")
        .withRow("patient-3", "2025-06-21")
        .build();
    final ElementPath datePath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.DATE)
        .dataset(dateDataset)
        .idAndValueColumns()
        .singular(true)
        .build();

    final Dataset<Row> timeDataset = new DatasetBuilder(spark)
        .withIdColumn(ID_ALIAS)
        .withColumn(DataTypes.StringType)
        .withRow("patient-1", "13:28:17")
        .withRow("patient-2", "08:00")
        .withRow("patient-3", "00")
        .build();
    final ElementPath timePath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.TIME)
        .dataset(timeDataset)
        .idAndValueColumns()
        .singular(true)
        .build();

    final Dataset<Row> instantDataset = new DatasetBuilder(spark)
        .withIdColumn(ID_ALIAS)
        .withColumn(DataTypes.TimestampType)
        .withRow("patient-1", Instant.ofEpochMilli(1667690454622L))
        .withRow("patient-2", Instant.ofEpochMilli(1667690454622L))
        .withRow("patient-3", Instant.ofEpochMilli(1667690454622L))
        .build();
    final ElementPath instantPath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.DATETIME)
        .dataset(instantDataset)
        .idAndValueColumns()
        .singular(true)
        .build();

    final DateTimeLiteralPath dateTimeLiteral = DateTimeLiteralPath.fromString(
        "@2015-02-07T18:28:17+00:00", dateTimePath);
    final DateLiteralPath dateLiteral = DateLiteralPath.fromString("@2015-02-07", datePath);
    final TimeLiteralPath timeLiteral = TimeLiteralPath.fromString("@T08:00", timePath);

    final ParserContext context = new ParserContextBuilder(spark, fhirContext)
        .groupingColumns(Collections.singletonList(dateTimePath.getIdColumn()))
        .build();

    parameters.addAll(dateTimeAddition(dateTimePath, context));
    parameters.addAll(dateTimeSubtraction(dateTimePath, context));
    parameters.addAll(dateAddition(datePath, context));
    parameters.addAll(dateSubtraction(datePath, context));
    parameters.addAll(instantAddition(instantPath, context));
    parameters.addAll(instantSubtraction(instantPath, context));
    parameters.addAll(dateTimeLiteralAddition(dateTimeLiteral, context));
    parameters.addAll(dateTimeLiteralSubtraction(dateTimeLiteral, context));
    parameters.addAll(dateLiteralAddition(dateLiteral, context));
    parameters.addAll(dateLiteralSubtraction(dateLiteral, context));

    return parameters.stream();
  }

  Collection<TestParameters> dateTimeAddition(
      final FhirPath dateTimePath, final ParserContext context) {
    final List<TestParameters> parameters = new ArrayList<>();
    parameters.add(new TestParameters("DateTime + 10 years", dateTimePath,
        QuantityLiteralPath.fromCalendarDurationString("10 years", dateTimePath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2025-02-07T13:28:17.000-05:00")
            .withRow("patient-2", "2027-01-01T00:00:00Z")
            .withRow("patient-3", "2035-06-21T00:15:00+10:00")
            .build())
    );

    parameters.add(new TestParameters("DateTime + 9 months", dateTimePath,
        QuantityLiteralPath.fromCalendarDurationString("9 months", dateTimePath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-11-07T13:28:17.000-05:00")
            .withRow("patient-2", "2017-10-01T00:00:00Z")
            .withRow("patient-3", "2026-03-21T00:15:00+10:00")
            .build())
    );

    parameters.add(new TestParameters("DateTime + 30 days", dateTimePath,
        QuantityLiteralPath.fromCalendarDurationString("30 days", dateTimePath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-03-09T13:28:17.000-05:00")
            .withRow("patient-2", "2017-01-31T00:00:00Z")
            .withRow("patient-3", "2025-07-21T00:15:00+10:00")
            .build())
    );

    parameters.add(new TestParameters("DateTime + 12 hours", dateTimePath,
        QuantityLiteralPath.fromCalendarDurationString("12 hours", dateTimePath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-08T01:28:17.000-05:00")
            .withRow("patient-2", "2017-01-01T12:00:00Z")
            .withRow("patient-3", "2025-06-21T12:15:00+10:00")
            .build())
    );

    parameters.add(new TestParameters("DateTime + 30 minutes", dateTimePath,
        QuantityLiteralPath.fromCalendarDurationString("30 minutes", dateTimePath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T13:58:17.000-05:00")
            .withRow("patient-2", "2017-01-01T00:30:00Z")
            .withRow("patient-3", "2025-06-21T00:45:00+10:00")
            .build())
    );

    parameters.add(new TestParameters("DateTime + 10 seconds", dateTimePath,
        QuantityLiteralPath.fromCalendarDurationString("10 seconds", dateTimePath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T13:28:27.000-05:00")
            .withRow("patient-2", "2017-01-01T00:00:10Z")
            .withRow("patient-3", "2025-06-21T00:15:10+10:00")
            .build())
    );

    parameters.add(new TestParameters("DateTime + 300 milliseconds", dateTimePath,
        QuantityLiteralPath.fromCalendarDurationString("300 milliseconds", dateTimePath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T13:28:17.300-05:00")
            .withRow("patient-2", "2017-01-01T00:00:00Z")
            .withRow("patient-3", "2025-06-21T00:15:00+10:00")
            .build())
    );
    return parameters;
  }

  Collection<TestParameters> dateTimeSubtraction(
      final FhirPath dateTimePath, final ParserContext context) {
    final List<TestParameters> parameters = new ArrayList<>();
    parameters.add(new TestParameters("DateTime - 10 years", dateTimePath,
        QuantityLiteralPath.fromCalendarDurationString("10 years", dateTimePath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2005-02-07T13:28:17.000-05:00")
            .withRow("patient-2", "2007-01-01T00:00:00Z")
            .withRow("patient-3", "2015-06-21T00:15:00+10:00")
            .build())
    );

    parameters.add(new TestParameters("DateTime - 9 months", dateTimePath,
        QuantityLiteralPath.fromCalendarDurationString("9 months", dateTimePath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2014-05-07T13:28:17.000-05:00")
            .withRow("patient-2", "2016-04-01T00:00:00Z")
            .withRow("patient-3", "2024-09-21T00:15:00+10:00")
            .build())
    );

    parameters.add(new TestParameters("DateTime - 30 days", dateTimePath,
        QuantityLiteralPath.fromCalendarDurationString("30 days", dateTimePath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-01-08T13:28:17.000-05:00")
            .withRow("patient-2", "2016-12-02T00:00:00Z")
            .withRow("patient-3", "2025-05-22T00:15:00+10:00")
            .build())
    );

    parameters.add(new TestParameters("DateTime - 12 hours", dateTimePath,
        QuantityLiteralPath.fromCalendarDurationString("12 hours", dateTimePath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T01:28:17.000-05:00")
            .withRow("patient-2", "2016-12-31T12:00:00Z")
            .withRow("patient-3", "2025-06-20T12:15:00+10:00")
            .build())
    );

    parameters.add(new TestParameters("DateTime - 30 minutes", dateTimePath,
        QuantityLiteralPath.fromCalendarDurationString("30 minutes", dateTimePath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T12:58:17.000-05:00")
            .withRow("patient-2", "2016-12-31T23:30:00Z")
            .withRow("patient-3", "2025-06-20T23:45:00+10:00")
            .build())
    );

    parameters.add(new TestParameters("DateTime - 10 seconds", dateTimePath,
        QuantityLiteralPath.fromCalendarDurationString("10 seconds", dateTimePath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T13:28:07.000-05:00")
            .withRow("patient-2", "2016-12-31T23:59:50Z")
            .withRow("patient-3", "2025-06-21T00:14:50+10:00")
            .build())
    );

    parameters.add(new TestParameters("DateTime - 300 milliseconds", dateTimePath,
        QuantityLiteralPath.fromCalendarDurationString("300 milliseconds", dateTimePath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T13:28:16.700-05:00")
            .withRow("patient-2", "2016-12-31T23:59:59Z")
            .withRow("patient-3", "2025-06-21T00:14:59+10:00")
            .build())
    );
    return parameters;
  }

  Collection<TestParameters> dateAddition(
      final FhirPath datePath, final ParserContext context) {
    final List<TestParameters> parameters = new ArrayList<>();
    parameters.add(new TestParameters("Date + 10 years", datePath,
        QuantityLiteralPath.fromCalendarDurationString("10 years", datePath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2025-02-07")
            .withRow("patient-2", "2027-01-01")
            .withRow("patient-3", "2035-06-21")
            .build())
    );

    parameters.add(new TestParameters("Date + 9 months", datePath,
        QuantityLiteralPath.fromCalendarDurationString("9 months", datePath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-11-07")
            .withRow("patient-2", "2017-10-01")
            .withRow("patient-3", "2026-03-21")
            .build())
    );

    parameters.add(new TestParameters("Date + 30 days", datePath,
        QuantityLiteralPath.fromCalendarDurationString("30 days", datePath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-03-09")
            .withRow("patient-2", "2017-01-31")
            .withRow("patient-3", "2025-07-21")
            .build())
    );

    return parameters;
  }

  Collection<TestParameters> dateSubtraction(
      final FhirPath datePath, final ParserContext context) {
    final List<TestParameters> parameters = new ArrayList<>();
    parameters.add(new TestParameters("Date - 10 years", datePath,
        QuantityLiteralPath.fromCalendarDurationString("10 years", datePath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2005-02-07")
            .withRow("patient-2", "2007-01-01")
            .withRow("patient-3", "2015-06-21")
            .build())
    );

    parameters.add(new TestParameters("Date - 9 months", datePath,
        QuantityLiteralPath.fromCalendarDurationString("9 months", datePath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2014-05-07")
            .withRow("patient-2", "2016-04-01")
            .withRow("patient-3", "2024-09-21")
            .build())
    );

    parameters.add(new TestParameters("Date - 30 days", datePath,
        QuantityLiteralPath.fromCalendarDurationString("30 days", datePath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-01-08")
            .withRow("patient-2", "2016-12-02")
            .withRow("patient-3", "2025-05-22")
            .build())
    );

    return parameters;
  }

  Collection<TestParameters> instantAddition(
      final FhirPath instantPath, final ParserContext context) {
    final List<TestParameters> parameters = new ArrayList<>();
    parameters.add(new TestParameters("Instant + 10 years", instantPath,
        QuantityLiteralPath.fromCalendarDurationString("10 years", instantPath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2032-11-05T23:20:54+00:00")
            .withRow("patient-2", "2032-11-05T23:20:54+00:00")
            .withRow("patient-3", "2032-11-05T23:20:54+00:00")
            .build())
    );

    parameters.add(new TestParameters("Instant + 9 months", instantPath,
        QuantityLiteralPath.fromCalendarDurationString("9 months", instantPath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2023-08-05T23:20:54+00:00")
            .withRow("patient-2", "2023-08-05T23:20:54+00:00")
            .withRow("patient-3", "2023-08-05T23:20:54+00:00")
            .build())
    );

    parameters.add(new TestParameters("Instant + 30 days", instantPath,
        QuantityLiteralPath.fromCalendarDurationString("30 days", instantPath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2022-12-05T23:20:54+00:00")
            .withRow("patient-2", "2022-12-05T23:20:54+00:00")
            .withRow("patient-3", "2022-12-05T23:20:54+00:00")
            .build())
    );

    parameters.add(new TestParameters("Instant + 12 hours", instantPath,
        QuantityLiteralPath.fromCalendarDurationString("12 hours", instantPath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2022-11-06T11:20:54+00:00")
            .withRow("patient-2", "2022-11-06T11:20:54+00:00")
            .withRow("patient-3", "2022-11-06T11:20:54+00:00")
            .build())
    );

    parameters.add(new TestParameters("Instant + 30 minutes", instantPath,
        QuantityLiteralPath.fromCalendarDurationString("30 minutes", instantPath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2022-11-05T23:50:54+00:00")
            .withRow("patient-2", "2022-11-05T23:50:54+00:00")
            .withRow("patient-3", "2022-11-05T23:50:54+00:00")
            .build())
    );

    parameters.add(new TestParameters("Instant + 10 seconds", instantPath,
        QuantityLiteralPath.fromCalendarDurationString("10 seconds", instantPath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2022-11-05T23:21:04+00:00")
            .withRow("patient-2", "2022-11-05T23:21:04+00:00")
            .withRow("patient-3", "2022-11-05T23:21:04+00:00")
            .build())
    );

    parameters.add(new TestParameters("Instant + 300 milliseconds", instantPath,
        QuantityLiteralPath.fromCalendarDurationString("300 milliseconds", instantPath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2022-11-05T23:20:54+00:00")
            .withRow("patient-2", "2022-11-05T23:20:54+00:00")
            .withRow("patient-3", "2022-11-05T23:20:54+00:00")
            .build())
    );
    return parameters;
  }

  Collection<TestParameters> instantSubtraction(
      final FhirPath instantPath, final ParserContext context) {
    final List<TestParameters> parameters = new ArrayList<>();
    parameters.add(new TestParameters("Instant - 10 years", instantPath,
        QuantityLiteralPath.fromCalendarDurationString("10 years", instantPath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2012-11-05T23:20:54+00:00")
            .withRow("patient-2", "2012-11-05T23:20:54+00:00")
            .withRow("patient-3", "2012-11-05T23:20:54+00:00")
            .build())
    );

    parameters.add(new TestParameters("Instant - 9 months", instantPath,
        QuantityLiteralPath.fromCalendarDurationString("9 months", instantPath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2022-02-05T23:20:54+00:00")
            .withRow("patient-2", "2022-02-05T23:20:54+00:00")
            .withRow("patient-3", "2022-02-05T23:20:54+00:00")
            .build())
    );

    parameters.add(new TestParameters("Instant - 30 days", instantPath,
        QuantityLiteralPath.fromCalendarDurationString("30 days", instantPath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2022-10-06T23:20:54+00:00")
            .withRow("patient-2", "2022-10-06T23:20:54+00:00")
            .withRow("patient-3", "2022-10-06T23:20:54+00:00")
            .build())
    );

    parameters.add(new TestParameters("Instant - 12 hours", instantPath,
        QuantityLiteralPath.fromCalendarDurationString("12 hours", instantPath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2022-11-05T11:20:54+00:00")
            .withRow("patient-2", "2022-11-05T11:20:54+00:00")
            .withRow("patient-3", "2022-11-05T11:20:54+00:00")
            .build())
    );

    parameters.add(new TestParameters("Instant - 30 minutes", instantPath,
        QuantityLiteralPath.fromCalendarDurationString("30 minutes", instantPath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2022-11-05T22:50:54+00:00")
            .withRow("patient-2", "2022-11-05T22:50:54+00:00")
            .withRow("patient-3", "2022-11-05T22:50:54+00:00")
            .build())
    );

    parameters.add(new TestParameters("Instant - 10 seconds", instantPath,
        QuantityLiteralPath.fromCalendarDurationString("10 seconds", instantPath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2022-11-05T23:20:44+00:00")
            .withRow("patient-2", "2022-11-05T23:20:44+00:00")
            .withRow("patient-3", "2022-11-05T23:20:44+00:00")
            .build())
    );

    parameters.add(new TestParameters("Instant - 300 milliseconds", instantPath,
        QuantityLiteralPath.fromCalendarDurationString("300 milliseconds", instantPath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2022-11-05T23:20:53+00:00")
            .withRow("patient-2", "2022-11-05T23:20:53+00:00")
            .withRow("patient-3", "2022-11-05T23:20:53+00:00")
            .build())
    );
    return parameters;
  }

  Collection<TestParameters> dateTimeLiteralAddition(
      final FhirPath dateTimeLiteralPath, final ParserContext context) {
    final List<TestParameters> parameters = new ArrayList<>();
    parameters.add(new TestParameters("@2015-02-07T18:28:17+00:00 + 10 years", dateTimeLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("10 years", dateTimeLiteralPath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2025-02-07T18:28:17+00:00")
            .withRow("patient-2", "2025-02-07T18:28:17+00:00")
            .withRow("patient-3", "2025-02-07T18:28:17+00:00")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07T18:28:17+00:00 + 9 months", dateTimeLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("9 months", dateTimeLiteralPath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-11-07T18:28:17+00:00")
            .withRow("patient-2", "2015-11-07T18:28:17+00:00")
            .withRow("patient-3", "2015-11-07T18:28:17+00:00")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07T18:28:17+00:00 + 30 days", dateTimeLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("30 days", dateTimeLiteralPath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-03-09T18:28:17+00:00")
            .withRow("patient-2", "2015-03-09T18:28:17+00:00")
            .withRow("patient-3", "2015-03-09T18:28:17+00:00")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07T18:28:17+00:00 + 12 hours", dateTimeLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("12 hours", dateTimeLiteralPath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-08T06:28:17+00:00")
            .withRow("patient-2", "2015-02-08T06:28:17+00:00")
            .withRow("patient-3", "2015-02-08T06:28:17+00:00")
            .build())
    );

    parameters.add(
        new TestParameters("@2015-02-07T18:28:17+00:00 + 30 minutes", dateTimeLiteralPath,
            QuantityLiteralPath.fromCalendarDurationString("30 minutes", dateTimeLiteralPath),
            context,
            Operator.getInstance("+"),
            new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
                .withRow("patient-1", "2015-02-07T18:58:17+00:00")
                .withRow("patient-2", "2015-02-07T18:58:17+00:00")
                .withRow("patient-3", "2015-02-07T18:58:17+00:00")
                .build())
    );

    parameters.add(
        new TestParameters("@2015-02-07T18:28:17+00:00 + 10 seconds", dateTimeLiteralPath,
            QuantityLiteralPath.fromCalendarDurationString("10 seconds", dateTimeLiteralPath),
            context,
            Operator.getInstance("+"),
            new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
                .withRow("patient-1", "2015-02-07T18:28:27+00:00")
                .withRow("patient-2", "2015-02-07T18:28:27+00:00")
                .withRow("patient-3", "2015-02-07T18:28:27+00:00")
                .build())
    );

    parameters.add(
        new TestParameters("@2015-02-07T18:28:17+00:00 + 300 milliseconds", dateTimeLiteralPath,
            QuantityLiteralPath.fromCalendarDurationString("300 milliseconds", dateTimeLiteralPath),
            context,
            Operator.getInstance("+"),
            new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
                .withRow("patient-1", "2015-02-07T18:28:17+00:00")
                .withRow("patient-2", "2015-02-07T18:28:17+00:00")
                .withRow("patient-3", "2015-02-07T18:28:17+00:00")
                .build())
    );

    return parameters;
  }

  Collection<TestParameters> dateTimeLiteralSubtraction(
      final FhirPath dateTimeLiteralPath, final ParserContext context) {
    final List<TestParameters> parameters = new ArrayList<>();
    parameters.add(new TestParameters("@2015-02-07T18:28:17+00:00 - 10 years", dateTimeLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("10 years", dateTimeLiteralPath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2005-02-07T18:28:17+00:00")
            .withRow("patient-2", "2005-02-07T18:28:17+00:00")
            .withRow("patient-3", "2005-02-07T18:28:17+00:00")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07T18:28:17+00:00 - 9 months", dateTimeLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("9 months", dateTimeLiteralPath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2014-05-07T18:28:17+00:00")
            .withRow("patient-2", "2014-05-07T18:28:17+00:00")
            .withRow("patient-3", "2014-05-07T18:28:17+00:00")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07T18:28:17+00:00 - 30 days", dateTimeLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("30 days", dateTimeLiteralPath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-01-08T18:28:17+00:00")
            .withRow("patient-2", "2015-01-08T18:28:17+00:00")
            .withRow("patient-3", "2015-01-08T18:28:17+00:00")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07T18:28:17+00:00 - 12 hours", dateTimeLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("12 hours", dateTimeLiteralPath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T06:28:17+00:00")
            .withRow("patient-2", "2015-02-07T06:28:17+00:00")
            .withRow("patient-3", "2015-02-07T06:28:17+00:00")
            .build())
    );

    parameters.add(
        new TestParameters("@2015-02-07T18:28:17+00:00 - 30 minutes", dateTimeLiteralPath,
            QuantityLiteralPath.fromCalendarDurationString("30 minutes", dateTimeLiteralPath),
            context,
            Operator.getInstance("-"),
            new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
                .withRow("patient-1", "2015-02-07T17:58:17+00:00")
                .withRow("patient-2", "2015-02-07T17:58:17+00:00")
                .withRow("patient-3", "2015-02-07T17:58:17+00:00")
                .build())
    );

    parameters.add(
        new TestParameters("@2015-02-07T18:28:17+00:00 - 10 seconds", dateTimeLiteralPath,
            QuantityLiteralPath.fromCalendarDurationString("10 seconds", dateTimeLiteralPath),
            context,
            Operator.getInstance("-"),
            new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
                .withRow("patient-1", "2015-02-07T18:28:07+00:00")
                .withRow("patient-2", "2015-02-07T18:28:07+00:00")
                .withRow("patient-3", "2015-02-07T18:28:07+00:00")
                .build())
    );

    parameters.add(
        new TestParameters("@2015-02-07T18:28:17+00:00 - 300 milliseconds", dateTimeLiteralPath,
            QuantityLiteralPath.fromCalendarDurationString("300 milliseconds", dateTimeLiteralPath),
            context,
            Operator.getInstance("-"),
            new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
                .withRow("patient-1", "2015-02-07T18:28:16+00:00")
                .withRow("patient-2", "2015-02-07T18:28:16+00:00")
                .withRow("patient-3", "2015-02-07T18:28:16+00:00")
                .build())
    );

    return parameters;
  }

  Collection<TestParameters> dateLiteralAddition(
      final FhirPath dateLiteralPath, final ParserContext context) {
    final List<TestParameters> parameters = new ArrayList<>();
    parameters.add(new TestParameters("@2015-02-07 + 10 years", dateLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("10 years", dateLiteralPath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2025-02-07")
            .withRow("patient-2", "2025-02-07")
            .withRow("patient-3", "2025-02-07")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07 + 9 months", dateLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("9 months", dateLiteralPath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-11-07")
            .withRow("patient-2", "2015-11-07")
            .withRow("patient-3", "2015-11-07")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07 + 30 days", dateLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("30 days", dateLiteralPath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-03-09")
            .withRow("patient-2", "2015-03-09")
            .withRow("patient-3", "2015-03-09")
            .build())
    );

    return parameters;
  }

  Collection<TestParameters> dateLiteralSubtraction(
      final FhirPath dateLiteralPath, final ParserContext context) {
    final List<TestParameters> parameters = new ArrayList<>();
    parameters.add(new TestParameters("@2015-02-07 - 10 years", dateLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("10 years", dateLiteralPath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2005-02-07")
            .withRow("patient-2", "2005-02-07")
            .withRow("patient-3", "2005-02-07")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07 - 9 months", dateLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("9 months", dateLiteralPath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2014-05-07")
            .withRow("patient-2", "2014-05-07")
            .withRow("patient-3", "2014-05-07")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07 - 30 days", dateLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("30 days", dateLiteralPath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-01-08")
            .withRow("patient-2", "2015-01-08")
            .withRow("patient-3", "2015-01-08")
            .build())
    );

    return parameters;
  }

  @ParameterizedTest
  @MethodSource("parameters")
  void test(@Nonnull final TestParameters parameters) {
    final OperatorInput input = new OperatorInput(parameters.getContext(),
        parameters.getLeft(), parameters.getRight());
    final FhirPath result = parameters.getOperator().invoke(input);
    assertThat(result).selectOrderedResult().hasRows(parameters.getExpectedResult());
  }

}
