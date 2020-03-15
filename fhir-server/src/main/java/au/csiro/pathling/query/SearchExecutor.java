/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query;

import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.parser.ExpressionParser;
import au.csiro.pathling.query.parsing.parser.ExpressionParserContext;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.param.StringAndListParam;
import ca.uhn.fhir.rest.param.StringOrListParam;
import ca.uhn.fhir.rest.param.StringParam;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.instance.model.api.IPrimitiveType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates the execution of a search query, implemented as an IBundleProvider for integration
 * into HAPI's mechanism for returning paged search results.
 *
 * @author John Grimes
 */
public class SearchExecutor extends QueryExecutor implements IBundleProvider {

  private static final Logger logger = LoggerFactory.getLogger(SearchExecutor.class);
  private final ResourceType subjectResource;
  private final StringAndListParam filters;
  private Dataset<Row> result;
  private Integer count;

  public SearchExecutor(ExecutorConfiguration configuration, ResourceType subjectResource,
      StringAndListParam filters) {
    super(configuration);
    this.subjectResource = subjectResource;
    this.filters = filters;
    String filterStrings = filters == null
                           ? "null"
                           : filters.getValuesAsQueryTokens().stream()
                               .map(andParam -> andParam.getValuesAsQueryTokens().stream()
                                   .map(StringParam::getValue)
                                   .collect(Collectors.joining(",")))
                               .collect(Collectors.joining(" & "));
    logger.info("Received search request: filters=[" + filterStrings + "]");
    initializeDataset();
  }

  @Override
  public IPrimitiveType<Date> getPublished() {
    return null;
  }

  @Nonnull
  @Override
  public List<IBaseResource> getResources(int theFromIndex, int theToIndex) {
    logger.info("Retrieving search results (" + (theFromIndex + 1) + "-" + theToIndex + ")");

    Dataset<Row> resources = result;
    if (theFromIndex != 0) {
      // Spark does not have an "offset" concept, so we create a list of rows to exclude and
      // subtract them from the dataset using a left anti-join.
      Dataset<Row> exclude = resources.limit(theFromIndex)
          .select(resources.col("id").alias("excludeId"));
      Column joinCondition = resources.col("id").equalTo(exclude.col("excludeId"));
      resources = resources.join(exclude, joinCondition, "left_anti");
    }
    // The dataset is trimmed to the requested size.
    if (theToIndex != 0) {
      resources = resources.limit(theToIndex - theFromIndex);
    }

    // The requested resources are encoded into HAPI FHIR objects, and then collected.
    assert configuration.getFhirEncoders() != null;
    ExpressionEncoder<IBaseResource> encoder = configuration.getFhirEncoders()
        .of(subjectResource.toCode());
    reportQueryPlan(resources);
    return resources.as(encoder).collectAsList();
  }

  private void reportQueryPlan(Dataset<Row> resources) {
    if (configuration.isExplainQueries()) {
      logger.info("Search query plan:");
      resources.explain(true);
    }
  }

  @Nullable
  @Override
  public String getUuid() {
    return null;
  }

  @Override
  public Integer preferredPageSize() {
    return null;
  }

  @Nullable
  @Override
  public Integer size() {
    if (count == null) {
      reportQueryPlan(result);
      count = Math.toIntExact(result.count());
    }
    return count;
  }

  private void initializeDataset() {
    ExpressionParserContext context = buildParserContext(subjectResource);

    if (filters == null || filters.getValuesAsQueryTokens().isEmpty()) {
      // If there are no filters, return all resources.
      result = subjectDataset;
    } else {
      ExpressionParser expressionParser = new ExpressionParser(context);
      Column filterColumn = null, idColumn = null;
      List<ParsedExpression> filterExpressions = new ArrayList<>();

      // Parse each of the supplied filter expressions, building up a filter column. The nested
      // loops here are to capture the AND/OR conditions possible through the FHIR API, see
      // https://hl7.org/fhir/R4/search.html#combining.
      for (StringOrListParam orParam : this.filters.getValuesAsQueryTokens()) {
        Column innerColumn = null;
        for (StringParam param : orParam.getValuesAsQueryTokens()) {
          ParsedExpression expression = expressionParser.parse(param.getValue());
          filterExpressions.add(expression);
          innerColumn = innerColumn == null
                        ? expression.getValueColumn()
                        : innerColumn.or(expression.getValueColumn());
          // We save away the first encountered ID column so that we can use it later to join the
          // subject resource dataset with the joined filter datasets.
          if (idColumn == null) {
            idColumn = expression.getIdColumn();
          }
        }
        filterColumn = filterColumn == null
                       ? innerColumn
                       : filterColumn.and(innerColumn);

        // Join all of the datasets from the parsed filter expressions together.
        Dataset<Row> filterDataset = joinExpressions(filterExpressions).where(filterColumn);
        // Get the full resources which are present in the filtered dataset.
        result = subjectDataset.alias("subject").join(filterDataset,
            subjectDataset.col("id").equalTo(idColumn), "left_semi");
        // We cache the dataset because we know it will be accessed for both the total and the
        // record retrieval.
        result.cache();
      }
    }
  }

}
