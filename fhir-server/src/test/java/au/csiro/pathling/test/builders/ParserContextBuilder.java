/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.builders;

import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.test.DefaultAnswer;
import ca.uhn.fhir.context.FhirContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.mockito.Mockito;

/**
 * @author John Grimes
 */
public class ParserContextBuilder {

  @Nullable
  private FhirPath inputContext;

  @Nullable
  private String inputExpression;

  @Nonnull
  private final FhirContext fhirContext;

  @Nonnull
  private final SparkSession spark;

  @Nonnull
  private Database database;

  @Nullable
  private TerminologyServiceFactory terminologyServiceFactory;

  @Nonnull
  private List<Column> groupingColumns;

  @Nonnull
  private final Map<String, Column> nodeIdColumns;

  public ParserContextBuilder(@Nonnull final SparkSession spark,
      @Nonnull final FhirContext fhirContext) {
    this.fhirContext = fhirContext;
    this.spark = spark;
    inputContext = null;
    database = Mockito.mock(Database.class, new DefaultAnswer());
    groupingColumns = Collections.emptyList();
    nodeIdColumns = new HashMap<>();
  }

  @Nonnull
  public ParserContextBuilder inputContext(@Nonnull final FhirPath inputContext) {
    this.inputContext = inputContext;
    return this;
  }

  @Nonnull
  public ParserContextBuilder inputExpression(@Nonnull final String inputExpression) {
    this.inputExpression = inputExpression;
    return this;
  }

  @Nonnull
  public ParserContextBuilder database(@Nonnull final Database database) {
    this.database = database;
    return this;
  }

  @Nonnull
  public ParserContextBuilder terminologyClientFactory(
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
    this.terminologyServiceFactory = terminologyServiceFactory;
    return this;
  }

  @Nonnull
  public ParserContextBuilder groupingColumns(@Nonnull final List<Column> groupingColumns) {
    this.groupingColumns = groupingColumns;
    return this;
  }

  @Nonnull
  public ParserContext build() {
    if (inputContext == null) {
      final Dataset<Row> dataset = new DatasetBuilder(spark)
          .withIdColumn("id")
          .withColumn(DataTypes.StringType)
          .build();
      final ResourcePathBuilder resourcePathBuilder = new ResourcePathBuilder(spark)
          .dataset(dataset);
      if (inputExpression != null) {
        resourcePathBuilder.expression(inputExpression);
      }
      inputContext = resourcePathBuilder.build();
    }
    return new ParserContext(inputContext, fhirContext, spark, database,
        Optional.ofNullable(terminologyServiceFactory), groupingColumns, nodeIdColumns);
  }

}
