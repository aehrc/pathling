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

package au.csiro.pathling.test.builders;

import static org.apache.spark.sql.functions.lit;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.DefaultAnswer;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;
import org.mockito.Mockito;

/**
 * @author John Grimes
 */
public class ParserContextBuilder {

  @Nonnull
  private FhirPath inputContext;

  @Nonnull
  private final FhirContext fhirContext;

  @Nonnull
  private final SparkSession spark;

  @Nonnull
  private DataSource dataSource;

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
    inputContext = mock(FhirPath.class);
    when(inputContext.getIdColumn()).thenReturn(lit(null));
    when(inputContext.getDataset()).thenReturn(spark.emptyDataFrame());
    dataSource = Mockito.mock(DataSource.class, new DefaultAnswer());
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
    when(inputContext.getExpression()).thenReturn(inputExpression);
    return this;
  }

  @Nonnull
  public ParserContextBuilder idColumn(@Nonnull final Column idColumn) {
    when(inputContext.getIdColumn()).thenReturn(idColumn);
    return this;
  }

  @Nonnull
  public ParserContextBuilder database(@Nonnull final DataSource dataSource) {
    this.dataSource = dataSource;
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
    return new ParserContext(inputContext, fhirContext, spark, dataSource,
        Optional.ofNullable(terminologyServiceFactory), groupingColumns, nodeIdColumns);
  }

}
