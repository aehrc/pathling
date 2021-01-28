/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql;


import au.csiro.pathling.test.assertions.DatasetAssert;
import au.csiro.pathling.test.builders.DatasetBuilder;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;


@SpringBootTest
@Tag("UnitTest")
public class SqlExtensionsTest {

  @Autowired
  private SparkSession spark;

  @Nullable
  private static String stringDecoder(@Nullable final Object value) {
    return value != null
           ? ((UTF8String) value).toString()
           : null;
  }

  /**
   * Collects all String values in a partition to a list and then maps each string to the index of
   * the this string in the list.
   */
  static class TestMapperWithPreview implements
      MapperWithPreview<String, Integer, List<String>> {

    private static final long serialVersionUID = -4978210449641957885L;

    @Override
    @Nonnull
    public List<String> preview(@Nonnull final Iterator<String> input) {
      final Iterable<String> iterable = () -> input;
      return StreamSupport
          .stream(iterable.spliterator(), false)
          .filter(Objects::nonNull)
          .collect(Collectors.toList());
    }

    @Override
    @Nullable
    public Integer call(@Nullable final String input, @Nonnull final List<String> state) {
      return input != null
             ? state.indexOf(input)
             : null;
    }
  }

  @Test
  public void testMapWithPartitionPreview() {
    final Dataset<Row> dataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withColumn("gender", DataTypes.StringType)
        .withColumn("active", DataTypes.BooleanType)
        .withRow("patient-1", "value0", true)
        .withRow("patient-2", "value1", false)
        .withRow("patient-3", "value2", true)
        .withRow("patient-4", null, true)
        .build().repartition(1);

    final Dataset<Row> resultDataset = SqlExtensions.mapWithPartitionPreview(dataset,
        dataset.col("gender"),
        SqlExtensionsTest::stringDecoder,
        new TestMapperWithPreview(),
        new StructField("myResult", DataTypes.IntegerType, true, Metadata.empty())
    );

    final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withColumn("gender", DataTypes.StringType)
        .withColumn("active", DataTypes.BooleanType)
        .withColumn("myResult", DataTypes.IntegerType)
        .withRow("patient-1", "value0", true, 0)
        .withRow("patient-2", "value1", false, 1)
        .withRow("patient-3", "value2", true, 2)
        .withRow("patient-4", null, true, null)
        .build();

    new DatasetAssert(resultDataset).hasRows(expectedDataset);
  }

}
