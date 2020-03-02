/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query;

import au.csiro.pathling.query.parsing.Joinable;
import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.parser.ExpressionParserContext;
import java.util.*;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Contains functionality common to query executors.
 *
 * @author John Grimes
 */
public abstract class QueryExecutor {

  protected final ExecutorConfiguration configuration;
  protected Dataset<Row> subjectDataset;

  public QueryExecutor(ExecutorConfiguration configuration) {
    this.configuration = configuration;
  }

  protected ExpressionParserContext buildParserContext(ResourceType subjectResourceType) {
    ExpressionParserContext parserContext = new ExpressionParserContext();

    parserContext.setFhirContext(configuration.getFhirContext());
    parserContext.setTerminologyClientFactory(configuration.getTerminologyClientFactory());
    parserContext.setTerminologyClient(configuration.getTerminologyClient());
    parserContext.setSparkSession(configuration.getSparkSession());
    parserContext.setResourceReader(configuration.getResourceReader());

    // Set up the subject resource dataset.
    String resourceCode = subjectResourceType.toCode();
    subjectDataset = parserContext.getResourceReader().read(subjectResourceType);
    String firstColumn = subjectDataset.columns()[0];
    String[] remainingColumns = Arrays
        .copyOfRange(subjectDataset.columns(), 1, subjectDataset.columns().length);
    Column idColumn = subjectDataset.col("id");
    Dataset<Row> expressionDataset = subjectDataset.withColumn("resource",
        functions.struct(firstColumn, remainingColumns));
    Column valueColumn = expressionDataset.col("resource");
    expressionDataset = expressionDataset.select(idColumn, valueColumn);

    // Create an expression for the subject resource.
    ParsedExpression subjectResource = new ParsedExpression();
    subjectResource.setFhirPath(resourceCode);
    subjectResource.setDataset(expressionDataset);
    subjectResource.setResource(true);
    subjectResource.setResourceType(subjectResourceType);
    subjectResource.setSingular(true);
    subjectResource.setOrigin(subjectResource);
    subjectResource.setHashedValue(idColumn, valueColumn);
    parserContext.setSubjectContext(subjectResource);

    return parserContext;
  }

  protected static Dataset<Row> joinExpressions(List<? extends Joinable> expressions) {
    if (expressions.isEmpty()) {
      throw new IllegalArgumentException("List of expressions must not be empty");
    }
    Joinable previous = expressions.get(0);
    Dataset<Row> result = previous.getDataset();
    Set<Dataset<Row>> joinedDatasets = new HashSet<>();
    joinedDatasets.add(result);
    for (int i = 0; i < expressions.size(); i++) {
      Joinable current = expressions.get(i);
      if (i > 0 && !joinedDatasets.contains(current.getDataset())) {
        result = result.join(current.getDataset(),
            previous.getIdColumn().equalTo(current.getIdColumn()), "inner");
        previous = current;
        joinedDatasets.add(current.getDataset());
      }
    }
    return result;
  }

  protected static Dataset<Row> applyFilters(Dataset<Row> dataset, List<ParsedExpression> filters) {
    Optional<Column> filterCondition = filters.stream()
        .map(ParsedExpression::getValueColumn)
        .reduce(Column::and);
    return filterCondition.isPresent()
        ? dataset.filter(filterCondition.get())
        : dataset;
  }

}
