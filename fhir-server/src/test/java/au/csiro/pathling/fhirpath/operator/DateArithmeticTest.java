/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.literal.DateLiteralPath;
import au.csiro.pathling.fhirpath.literal.DateTimeLiteralPath;
import au.csiro.pathling.fhirpath.literal.QuantityLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import ca.uhn.fhir.context.FhirContext;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
@TestInstance(Lifecycle.PER_CLASS)
@Tag("UnitTest")
public class DateArithmeticTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  static final String ID_ALIAS = "_abc123";
  static final List<String> OPERATORS = List.of("+", "-");

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
        .withRow("patient-1", "2015-02-07T13:28:17-05:00")
        .withRow("patient-2", "2017-01-01T00:00:00.000Z")
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

    final DateTimeLiteralPath dateTimeLiteral = DateTimeLiteralPath.fromString(
        "@2015-02-07T18:28:17.000Z", dateTimePath);
    final DateLiteralPath dateLiteral = DateLiteralPath.fromString("@2015-02-07", datePath);

    final ParserContext context = new ParserContextBuilder(spark, fhirContext)
        .groupingColumns(Collections.singletonList(dateTimePath.getIdColumn()))
        .build();

    parameters.addAll(dateTimeAddition(dateTimePath, context));
    parameters.addAll(dateTimeSubtraction(dateTimePath, context));
    parameters.addAll(dateAddition(datePath, context));
    parameters.addAll(dateSubtraction(datePath, context));
    parameters.addAll(dateTimeLiteralAddition(datePath, context));
    parameters.addAll(dateTimeLiteralSubtraction(datePath, context));
    parameters.addAll(dateLiteralAddition(datePath, context));
    parameters.addAll(dateLiteralSubtraction(datePath, context));

    return parameters.stream();
  }

  Collection<TestParameters> dateTimeAddition(
      final FhirPath dateTimePath, final ParserContext context) {
    final List<TestParameters> parameters = new ArrayList<>();
    parameters.add(new TestParameters("DateTime + 10 years", dateTimePath,
        QuantityLiteralPath.fromCalendarDurationString("10 years", dateTimePath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2025-02-07T18:28:17.000Z")
            .withRow("patient-2", "2027-01-01T00:00:00.000Z")
            .withRow("patient-3", "2035-06-20T14:15:00.000Z")
            .build())
    );

    parameters.add(new TestParameters("DateTime + 9 months", dateTimePath,
        QuantityLiteralPath.fromCalendarDurationString("9 months", dateTimePath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-11-07T18:28:17.000Z")
            .withRow("patient-2", "2017-10-01T00:00:00.000Z")
            .withRow("patient-3", "2026-03-20T14:15:00.000Z")
            .build())
    );

    parameters.add(new TestParameters("DateTime + 2 weeks", dateTimePath,
        QuantityLiteralPath.fromCalendarDurationString("2 weeks", dateTimePath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-21T18:28:17.000Z")
            .withRow("patient-2", "2017-01-15T00:00:00.000Z")
            .withRow("patient-3", "2025-07-04T14:15:00.000Z")
            .build())
    );

    parameters.add(new TestParameters("DateTime + 30 days", dateTimePath,
        QuantityLiteralPath.fromCalendarDurationString("30 days", dateTimePath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-03-09T18:28:17.000Z")
            .withRow("patient-2", "2017-01-31T00:00:00.000Z")
            .withRow("patient-3", "2025-07-20T14:15:00.000Z")
            .build())
    );

    parameters.add(new TestParameters("DateTime + 12 hours", dateTimePath,
        QuantityLiteralPath.fromCalendarDurationString("12 hours", dateTimePath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-08T06:28:17.000Z")
            .withRow("patient-2", "2017-01-01T12:00:00.000Z")
            .withRow("patient-3", "2025-06-21T12:15:00.000Z")
            .build())
    );

    parameters.add(new TestParameters("DateTime + 30 minutes", dateTimePath,
        QuantityLiteralPath.fromCalendarDurationString("30 minutes", dateTimePath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T18:58:17.000Z")
            .withRow("patient-2", "2017-01-01T00:30:00.000Z")
            .withRow("patient-3", "2025-06-20T14:45:00.000Z")
            .build())
    );

    parameters.add(new TestParameters("DateTime + 10 seconds", dateTimePath,
        QuantityLiteralPath.fromCalendarDurationString("10 seconds", dateTimePath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T18:28:27.000Z")
            .withRow("patient-2", "2017-01-01T00:00:10.000Z")
            .withRow("patient-3", "2025-06-20T14:15:10.000Z")
            .build())
    );

    parameters.add(new TestParameters("DateTime + 300 milliseconds", dateTimePath,
        QuantityLiteralPath.fromCalendarDurationString("300 milliseconds", dateTimePath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T18:28:17.300Z")
            .withRow("patient-2", "2017-01-01T00:00:00.300Z")
            .withRow("patient-3", "2025-06-20T14:15:00.300Z")
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
            .withRow("patient-1", "2005-02-07T18:28:17.000Z")
            .withRow("patient-2", "2007-01-01T00:00:00.000Z")
            .withRow("patient-3", "2015-06-20T14:15:00.000Z")
            .build())
    );

    parameters.add(new TestParameters("DateTime - 9 months", dateTimePath,
        QuantityLiteralPath.fromCalendarDurationString("9 months", dateTimePath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2014-05-07T18:28:17.000Z")
            .withRow("patient-2", "2016-04-01T00:00:00.000Z")
            .withRow("patient-3", "2024-09-20T14:15:00.000Z")
            .build())
    );

    parameters.add(new TestParameters("DateTime - 2 weeks", dateTimePath,
        QuantityLiteralPath.fromCalendarDurationString("2 weeks", dateTimePath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-01-24T18:28:17.000Z")
            .withRow("patient-2", "2016-12-18T00:00:00.000Z")
            .withRow("patient-3", "2025-06-06T14:15:00.000Z")
            .build())
    );

    parameters.add(new TestParameters("DateTime - 30 days", dateTimePath,
        QuantityLiteralPath.fromCalendarDurationString("30 days", dateTimePath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-01-08T18:28:17.000Z")
            .withRow("patient-2", "2016-12-02T00:00:00.000Z")
            .withRow("patient-3", "2025-05-21T14:15:00.000Z")
            .build())
    );

    parameters.add(new TestParameters("DateTime - 12 hours", dateTimePath,
        QuantityLiteralPath.fromCalendarDurationString("12 hours", dateTimePath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T06:28:17.000Z")
            .withRow("patient-2", "2016-12-31T12:00:00.000Z")
            .withRow("patient-3", "2025-06-20T02:15:00.000Z")
            .build())
    );

    parameters.add(new TestParameters("DateTime - 30 minutes", dateTimePath,
        QuantityLiteralPath.fromCalendarDurationString("30 minutes", dateTimePath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T17:58:17.000Z")
            .withRow("patient-2", "2016-12-31T23:30:00.000Z")
            .withRow("patient-3", "2025-06-20T13:45:00.000Z")
            .build())
    );

    parameters.add(new TestParameters("DateTime - 10 seconds", dateTimePath,
        QuantityLiteralPath.fromCalendarDurationString("10 seconds", dateTimePath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T18:28:07.000Z")
            .withRow("patient-2", "2016-12-31T23:59:50.000Z")
            .withRow("patient-3", "2025-06-20T14:14:50.000Z")
            .build())
    );

    parameters.add(new TestParameters("DateTime - 300 milliseconds", dateTimePath,
        QuantityLiteralPath.fromCalendarDurationString("300 milliseconds", dateTimePath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T18:28:16.700Z")
            .withRow("patient-2", "2016-12-31T23:59:59.700Z")
            .withRow("patient-3", "2025-06-20T14:14:59.700Z")
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
            .withRow("patient-1", "2025-02-07T18:28:17.000Z")
            .withRow("patient-2", "2027-01-01T00:00:00.000Z")
            .withRow("patient-3", "2035-06-20T14:15:00.000Z")
            .build())
    );

    parameters.add(new TestParameters("Date + 9 months", datePath,
        QuantityLiteralPath.fromCalendarDurationString("9 months", datePath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-11-07T18:28:17.000Z")
            .withRow("patient-2", "2017-10-01T00:00:00.000Z")
            .withRow("patient-3", "2026-03-20T14:15:00.000Z")
            .build())
    );

    parameters.add(new TestParameters("Date + 2 weeks", datePath,
        QuantityLiteralPath.fromCalendarDurationString("2 weeks", datePath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-21T18:28:17.000Z")
            .withRow("patient-2", "2017-01-15T00:00:00.000Z")
            .withRow("patient-3", "2025-07-04T14:15:00.000Z")
            .build())
    );

    parameters.add(new TestParameters("Date + 30 days", datePath,
        QuantityLiteralPath.fromCalendarDurationString("30 days", datePath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-03-09T18:28:17.000Z")
            .withRow("patient-2", "2017-01-31T00:00:00.000Z")
            .withRow("patient-3", "2025-07-20T14:15:00.000Z")
            .build())
    );

    parameters.add(new TestParameters("Date + 12 hours", datePath,
        QuantityLiteralPath.fromCalendarDurationString("12 hours", datePath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-08T06:28:17.000Z")
            .withRow("patient-2", "2017-01-01T12:00:00.000Z")
            .withRow("patient-3", "2025-06-21T12:15:00.000Z")
            .build())
    );

    parameters.add(new TestParameters("Date + 30 minutes", datePath,
        QuantityLiteralPath.fromCalendarDurationString("30 minutes", datePath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T18:58:17.000Z")
            .withRow("patient-2", "2017-01-01T00:30:00.000Z")
            .withRow("patient-3", "2025-06-20T14:45:00.000Z")
            .build())
    );

    parameters.add(new TestParameters("Date + 10 seconds", datePath,
        QuantityLiteralPath.fromCalendarDurationString("10 seconds", datePath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T18:28:27.000Z")
            .withRow("patient-2", "2017-01-01T00:00:10.000Z")
            .withRow("patient-3", "2025-06-20T14:15:10.000Z")
            .build())
    );

    parameters.add(new TestParameters("Date + 300 milliseconds", datePath,
        QuantityLiteralPath.fromCalendarDurationString("300 milliseconds", datePath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T18:28:17.300Z")
            .withRow("patient-2", "2017-01-01T00:00:00.300Z")
            .withRow("patient-3", "2025-06-20T14:15:00.300Z")
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
            .withRow("patient-1", "2005-02-07T18:28:17.000Z")
            .withRow("patient-2", "2007-01-01T00:00:00.000Z")
            .withRow("patient-3", "2015-06-20T14:15:00.000Z")
            .build())
    );

    parameters.add(new TestParameters("Date - 9 months", datePath,
        QuantityLiteralPath.fromCalendarDurationString("9 months", datePath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2014-05-07T18:28:17.000Z")
            .withRow("patient-2", "2016-04-01T00:00:00.000Z")
            .withRow("patient-3", "2024-09-20T14:15:00.000Z")
            .build())
    );

    parameters.add(new TestParameters("Date - 2 weeks", datePath,
        QuantityLiteralPath.fromCalendarDurationString("2 weeks", datePath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-01-24T18:28:17.000Z")
            .withRow("patient-2", "2016-12-18T00:00:00.000Z")
            .withRow("patient-3", "2025-06-06T14:15:00.000Z")
            .build())
    );

    parameters.add(new TestParameters("Date - 30 days", datePath,
        QuantityLiteralPath.fromCalendarDurationString("30 days", datePath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-01-08T18:28:17.000Z")
            .withRow("patient-2", "2016-12-02T00:00:00.000Z")
            .withRow("patient-3", "2025-05-21T14:15:00.000Z")
            .build())
    );

    parameters.add(new TestParameters("Date - 12 hours", datePath,
        QuantityLiteralPath.fromCalendarDurationString("12 hours", datePath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T06:28:17.000Z")
            .withRow("patient-2", "2016-12-31T12:00:00.000Z")
            .withRow("patient-3", "2025-06-20T02:15:00.000Z")
            .build())
    );

    parameters.add(new TestParameters("Date - 30 minutes", datePath,
        QuantityLiteralPath.fromCalendarDurationString("30 minutes", datePath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T17:58:17.000Z")
            .withRow("patient-2", "2016-12-31T23:30:00.000Z")
            .withRow("patient-3", "2025-06-20T13:45:00.000Z")
            .build())
    );

    parameters.add(new TestParameters("Date - 10 seconds", datePath,
        QuantityLiteralPath.fromCalendarDurationString("10 seconds", datePath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T18:28:07.000Z")
            .withRow("patient-2", "2016-12-31T23:59:50.000Z")
            .withRow("patient-3", "2025-06-20T14:14:50.000Z")
            .build())
    );

    parameters.add(new TestParameters("Date - 300 milliseconds", datePath,
        QuantityLiteralPath.fromCalendarDurationString("300 milliseconds", datePath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T18:28:16.700Z")
            .withRow("patient-2", "2016-12-31T23:59:59.700Z")
            .withRow("patient-3", "2025-06-20T14:14:59.700Z")
            .build())
    );
    return parameters;
  }

  Collection<TestParameters> dateTimeLiteralAddition(
      final FhirPath dateTimeLiteralPath, final ParserContext context) {
    final List<TestParameters> parameters = new ArrayList<>();
    parameters.add(new TestParameters("@2015-02-07T18:28:17.000Z + 10 years", dateTimeLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("10 years", dateTimeLiteralPath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2025-02-07T18:28:17.000Z")
            .withRow("patient-2", "2025-02-07T18:28:17.000Z")
            .withRow("patient-3", "2025-02-07T18:28:17.000Z")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07T18:28:17.000Z + 9 months", dateTimeLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("9 months", dateTimeLiteralPath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-11-07T18:28:17.000Z")
            .withRow("patient-2", "2015-11-07T18:28:17.000Z")
            .withRow("patient-3", "2015-11-07T18:28:17.000Z")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07T18:28:17.000Z + 2 weeks", dateTimeLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("2 weeks", dateTimeLiteralPath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-21T18:28:17.000Z")
            .withRow("patient-2", "2015-02-21T18:28:17.000Z")
            .withRow("patient-3", "2015-02-21T18:28:17.000Z")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07T18:28:17.000Z + 30 days", dateTimeLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("30 days", dateTimeLiteralPath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-03-09T18:28:17.000Z")
            .withRow("patient-2", "2015-03-09T18:28:17.000Z")
            .withRow("patient-3", "2015-03-09T18:28:17.000Z")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07T18:28:17.000Z + 12 hours", dateTimeLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("12 hours", dateTimeLiteralPath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-08T06:28:17.000Z")
            .withRow("patient-2", "2015-02-08T06:28:17.000Z")
            .withRow("patient-3", "2015-02-08T06:28:17.000Z")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07T18:28:17.000Z + 30 minutes", dateTimeLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("30 minutes", dateTimeLiteralPath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T18:58:17.000Z")
            .withRow("patient-2", "2015-02-07T18:58:17.000Z")
            .withRow("patient-3", "2015-02-07T18:58:17.000Z")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07T18:28:17.000Z + 10 seconds", dateTimeLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("10 seconds", dateTimeLiteralPath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T18:28:27.000Z")
            .withRow("patient-2", "2015-02-07T18:28:27.000Z")
            .withRow("patient-3", "2015-02-07T18:28:27.000Z")
            .build())
    );

    parameters.add(
        new TestParameters("@2015-02-07T18:28:17.000Z + 300 milliseconds", dateTimeLiteralPath,
            QuantityLiteralPath.fromCalendarDurationString("300 milliseconds", dateTimeLiteralPath),
            context,
            Operator.getInstance("+"),
            new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
                .withRow("patient-1", "2015-02-07T18:28:17.300Z")
                .withRow("patient-2", "2015-02-07T18:28:17.300Z")
                .withRow("patient-3", "2015-02-07T18:28:17.300Z")
                .build())
    );
    return parameters;
  }

  Collection<TestParameters> dateTimeLiteralSubtraction(
      final FhirPath dateTimeLiteralPath, final ParserContext context) {
    final List<TestParameters> parameters = new ArrayList<>();
    parameters.add(new TestParameters("@2015-02-07T18:28:17.000Z - 10 years", dateTimeLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("10 years", dateTimeLiteralPath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2005-02-07T18:28:17.000Z")
            .withRow("patient-2", "2005-02-07T18:28:17.000Z")
            .withRow("patient-3", "2005-02-07T18:28:17.000Z")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07T18:28:17.000Z - 9 months", dateTimeLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("9 months", dateTimeLiteralPath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2014-05-07T18:28:17.000Z")
            .withRow("patient-2", "2014-05-07T18:28:17.000Z")
            .withRow("patient-3", "2014-05-07T18:28:17.000Z")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07T18:28:17.000Z - 2 weeks", dateTimeLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("2 weeks", dateTimeLiteralPath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-01-24T18:28:17.000Z")
            .withRow("patient-2", "2015-01-24T18:28:17.000Z")
            .withRow("patient-3", "2015-01-24T18:28:17.000Z")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07T18:28:17.000Z - 30 days", dateTimeLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("30 days", dateTimeLiteralPath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-01-08T18:28:17.000Z")
            .withRow("patient-2", "2015-01-08T18:28:17.000Z")
            .withRow("patient-3", "2015-01-08T18:28:17.000Z")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07T18:28:17.000Z - 12 hours", dateTimeLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("12 hours", dateTimeLiteralPath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T06:28:17.000Z")
            .withRow("patient-2", "2015-02-07T06:28:17.000Z")
            .withRow("patient-3", "2015-02-07T06:28:17.000Z")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07T18:28:17.000Z - 30 minutes", dateTimeLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("30 minutes", dateTimeLiteralPath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T17:58:17.000Z")
            .withRow("patient-2", "2015-02-07T17:58:17.000Z")
            .withRow("patient-3", "2015-02-07T17:58:17.000Z")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07T18:28:17.000Z - 10 seconds", dateTimeLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("10 seconds", dateTimeLiteralPath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T18:28:07.000Z")
            .withRow("patient-2", "2015-02-07T18:28:07.000Z")
            .withRow("patient-3", "2015-02-07T18:28:07.000Z")
            .build())
    );

    parameters.add(
        new TestParameters("@2015-02-07T18:28:17.000Z - 300 milliseconds", dateTimeLiteralPath,
            QuantityLiteralPath.fromCalendarDurationString("300 milliseconds", dateTimeLiteralPath),
            context,
            Operator.getInstance("-"),
            new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
                .withRow("patient-1", "2015-02-07T18:28:16.700Z")
                .withRow("patient-2", "2015-02-07T18:28:16.700Z")
                .withRow("patient-3", "2015-02-07T18:28:16.700Z")
                .build())
    );
    return parameters;
  }

  Collection<TestParameters> dateLiteralAddition(
      final FhirPath dateLiteralPath, final ParserContext context) {
    final List<TestParameters> parameters = new ArrayList<>();
    parameters.add(new TestParameters("@2015-02-07T18:28:17.000Z + 10 years", dateLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("10 years", dateLiteralPath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2025-02-07T18:28:17.000Z")
            .withRow("patient-2", "2025-02-07T18:28:17.000Z")
            .withRow("patient-3", "2025-02-07T18:28:17.000Z")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07T18:28:17.000Z + 9 months", dateLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("9 months", dateLiteralPath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-11-07T18:28:17.000Z")
            .withRow("patient-2", "2015-11-07T18:28:17.000Z")
            .withRow("patient-3", "2015-11-07T18:28:17.000Z")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07T18:28:17.000Z + 2 weeks", dateLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("2 weeks", dateLiteralPath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-21T18:28:17.000Z")
            .withRow("patient-2", "2015-02-21T18:28:17.000Z")
            .withRow("patient-3", "2015-02-21T18:28:17.000Z")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07T18:28:17.000Z + 30 days", dateLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("30 days", dateLiteralPath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-03-09T18:28:17.000Z")
            .withRow("patient-2", "2015-03-09T18:28:17.000Z")
            .withRow("patient-3", "2015-03-09T18:28:17.000Z")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07T18:28:17.000Z + 12 hours", dateLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("12 hours", dateLiteralPath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-08T06:28:17.000Z")
            .withRow("patient-2", "2015-02-08T06:28:17.000Z")
            .withRow("patient-3", "2015-02-08T06:28:17.000Z")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07T18:28:17.000Z + 30 minutes", dateLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("30 minutes", dateLiteralPath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T18:58:17.000Z")
            .withRow("patient-2", "2015-02-07T18:58:17.000Z")
            .withRow("patient-3", "2015-02-07T18:58:17.000Z")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07T18:28:17.000Z + 10 seconds", dateLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("10 seconds", dateLiteralPath), context,
        Operator.getInstance("+"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T18:28:27.000Z")
            .withRow("patient-2", "2015-02-07T18:28:27.000Z")
            .withRow("patient-3", "2015-02-07T18:28:27.000Z")
            .build())
    );

    parameters.add(
        new TestParameters("@2015-02-07T18:28:17.000Z + 300 milliseconds", dateLiteralPath,
            QuantityLiteralPath.fromCalendarDurationString("300 milliseconds", dateLiteralPath),
            context,
            Operator.getInstance("+"),
            new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
                .withRow("patient-1", "2015-02-07T18:28:17.300Z")
                .withRow("patient-2", "2015-02-07T18:28:17.300Z")
                .withRow("patient-3", "2015-02-07T18:28:17.300Z")
                .build())
    );
    return parameters;
  }

  Collection<TestParameters> dateLiteralSubtraction(
      final FhirPath dateLiteralPath, final ParserContext context) {
    final List<TestParameters> parameters = new ArrayList<>();
    parameters.add(new TestParameters("@2015-02-07T18:28:17.000Z - 10 years", dateLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("10 years", dateLiteralPath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2005-02-07T18:28:17.000Z")
            .withRow("patient-2", "2005-02-07T18:28:17.000Z")
            .withRow("patient-3", "2005-02-07T18:28:17.000Z")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07T18:28:17.000Z - 9 months", dateLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("9 months", dateLiteralPath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2014-05-07T18:28:17.000Z")
            .withRow("patient-2", "2014-05-07T18:28:17.000Z")
            .withRow("patient-3", "2014-05-07T18:28:17.000Z")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07T18:28:17.000Z - 2 weeks", dateLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("2 weeks", dateLiteralPath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-01-24T18:28:17.000Z")
            .withRow("patient-2", "2015-01-24T18:28:17.000Z")
            .withRow("patient-3", "2015-01-24T18:28:17.000Z")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07T18:28:17.000Z - 30 days", dateLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("30 days", dateLiteralPath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-01-08T18:28:17.000Z")
            .withRow("patient-2", "2015-01-08T18:28:17.000Z")
            .withRow("patient-3", "2015-01-08T18:28:17.000Z")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07T18:28:17.000Z - 12 hours", dateLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("12 hours", dateLiteralPath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T06:28:17.000Z")
            .withRow("patient-2", "2015-02-07T06:28:17.000Z")
            .withRow("patient-3", "2015-02-07T06:28:17.000Z")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07T18:28:17.000Z - 30 minutes", dateLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("30 minutes", dateLiteralPath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T17:58:17.000Z")
            .withRow("patient-2", "2015-02-07T17:58:17.000Z")
            .withRow("patient-3", "2015-02-07T17:58:17.000Z")
            .build())
    );

    parameters.add(new TestParameters("@2015-02-07T18:28:17.000Z - 10 seconds", dateLiteralPath,
        QuantityLiteralPath.fromCalendarDurationString("10 seconds", dateLiteralPath), context,
        Operator.getInstance("-"),
        new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
            .withRow("patient-1", "2015-02-07T18:28:07.000Z")
            .withRow("patient-2", "2015-02-07T18:28:07.000Z")
            .withRow("patient-3", "2015-02-07T18:28:07.000Z")
            .build())
    );

    parameters.add(
        new TestParameters("@2015-02-07T18:28:17.000Z - 300 milliseconds", dateLiteralPath,
            QuantityLiteralPath.fromCalendarDurationString("300 milliseconds", dateLiteralPath),
            context,
            Operator.getInstance("-"),
            new DatasetBuilder(spark).withIdColumn(ID_ALIAS).withColumn(DataTypes.StringType)
                .withRow("patient-1", "2015-02-07T18:28:16.700Z")
                .withRow("patient-2", "2015-02-07T18:28:16.700Z")
                .withRow("patient-3", "2015-02-07T18:28:16.700Z")
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
