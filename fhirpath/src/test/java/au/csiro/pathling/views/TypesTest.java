package au.csiro.pathling.views;

import au.csiro.pathling.test.SpringBootUnitTest;
import java.util.Arrays;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser$;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

// TODO: remove

@SpringBootUnitTest
@Disabled
public class TypesTest {


  @Autowired
  SparkSession spark;

  @Test
  void testCharTypes() {

    final Dataset<Row> resultTS = spark.range(1).toDF()
        .select(functions.lit(null)
            .cast(DataTypes.NullType)
            .cast(DataTypes.StringType));

    resultTS.show(false);
  }

  @Test
  void testArrayType() {
    final Dataset<Row> inputDS = spark.range(1).toDF()
        .select(functions.array(
            functions.lit(1)
        ).alias("array_col"));

    DataTypes.createDayTimeIntervalType();
    inputDS.select(functions.col("array_col").cast(DataTypes.StringType))
        .show(false);

    inputDS.selectExpr("TRY_CAST(array_col AS INTERVAL) AS array_col")
        .show(false);
    inputDS.select(functions.col("array_col").cast(DataTypes.IntegerType))
        .show(false);

  }

  @Test
  void testTimestmapType() {

    spark.sql("SET TIME ZONE 'America/Los_Angeles'");
    //spark.sql("SET spark.sql.session.timeZone = America/Los_Angeles");

    spark.sql("SELECT current_timezone()").show();

    final Map<@NotNull String, @NotNull String> timestamps = Map.of(
        "ts_date", "2010-01-09",
        "ts_tz_UTC", "2010-01-09T19:04:32.323+00:00",
        "ts_tz_Z", "2010-01-09T19:04:32.323Z",
        "ts_tz_10", "2010-01-09T19:04:32.3232+10:00"
    );

    // final Dataset<Row> resultTS = spark.range(1).toDF()
    //     .select(timestamps.entrySet().stream()
    //         .map(entry -> {
    //           final String name = entry.getKey();
    //           final String value = entry.getValue();
    //           final Column column = functions.lit(value).cast(DataTypes.TimestampType);
    //           return column.alias(name);
    //         }).toArray(Column[]::new));
    //
    // resultTS.show(false);
    //
    // final Dataset<Row> resultTS_NTZ = spark.range(1).toDF()
    //     .select(timestamps.entrySet().stream()
    //         .map(entry -> {
    //           final String name = entry.getKey();
    //           final String value = entry.getValue();
    //           final Column column = functions.lit(value).cast(DataTypes.TimestampNTZType);
    //           return column.alias(name);
    //         }).toArray(Column[]::new));
    // resultTS_NTZ.show(false);

    final Dataset<Row> resultXX = spark.range(1).toDF()
        .select(
            functions.lit("2010-01-09T19:04:32.323+01:00").cast(DataTypes.TimestampType)
                .alias("ts_tz_UTC"),
            functions.to_utc_timestamp(
                functions.lit("2010-01-09T19:04:32.323+01:00").cast(DataTypes.TimestampType),
                functions.current_timezone()
            ),
            functions.from_utc_timestamp(
                functions.lit("2010-01-09T19:04:32.323+01:00").cast(DataTypes.TimestampType),
                "+10:00").alias("ts_tz_10"),
            functions.date_format(
                functions.from_utc_timestamp(
                    functions.lit("2010-01-09T19:04:32.323+01:00").cast(DataTypes.TimestampType),
                    "UTC"),
                "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("ts_tz_UTC_formatted")
            
        );

    resultXX.show(false);
  }

  @Test
  void testTypes() {

    // CHARACTER(n) / CHAR(n)
    // CHARACTER VARYING(n) / VARCHAR(n)
    // NUMERIC(p, s)
    // DECIMAL(p, s) / DEC(p, s)
    // SMALLINT
    // INTEGER / INT
    // BIGINT
    // FLOAT(p)
    // REAL
    // DOUBLE PRECISION
    // BOOLEAN
    // BINARY(n)
    // BINARY VARYING(n) / VARBINARY(n)
    // DATE
    // TIMESTAMP [(p)] [ WITHOUT TIME ZONE | WITH TIME ZONE ]
    // INTERVAL

    // create a list with strings respresenting all the above types
    final String[] types = {
        "CHARACTER(10)", "CHAR(10)", "CHARACTER VARYING(10)", "VARCHAR(10)",
        "NUMERIC(10, 2)", "DECIMAL(10, 2)",
        "DEC(10, 2)",  // DECIMAL synonym
        "SMALLINT", "INTEGER", "INT", "BIGINT",
        "FLOAT",
        "FLOAT(24)", "REAL", "DOUBLE PRECISION", "BOOLEAN", "BINARY(10)",
        "VARBINARY(10)", "DATE",
        "TIMESTAMP",
        "TIMESTAMP(3)",  // Example with precision
        "TIMESTAMP WITHOUT TIME ZONE",
        "TIMESTAMP WITH TIME ZONE", "INTERVAL"
    };

    Arrays.stream(types).forEach(type -> {
      try {
        // Parse the type using Spark's Catalyst SQL parser
        DataType dataType = CatalystSqlParser$.MODULE$.parseDataType(type);
        System.out.println(type + " -> " + dataType + " -> " + dataType.sql());
      } catch (Exception e) {
        // If parsing fails, print the error
        System.out.println(type + " -> " + "!!! ERROR: ");
      }
    });
  }
}
