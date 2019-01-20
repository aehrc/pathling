/*
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight.datasource;

import static au.csiro.clinsight.datasource.QueryUtilities.quote;
import static au.csiro.clinsight.fhir.FhirUtilities.getIdFromReference;
import static au.csiro.clinsight.persistence.Naming.tableNameForDimension;
import static java.util.stream.Collectors.joining;

import au.csiro.clinsight.fhir.FhirUtilities;
import au.csiro.clinsight.persistence.Dimension;
import au.csiro.clinsight.persistence.DimensionAttribute;
import au.csiro.clinsight.persistence.Metric;
import au.csiro.clinsight.persistence.Naming;
import au.csiro.clinsight.persistence.Query;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.model.primitive.IdDt;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.hibernate.Session;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class SqlQueryTranslator implements QueryTranslator<String> {

  Session session;
  IParser jsonParser;

  @Autowired
  public SqlQueryTranslator(FhirContext fhirContext, Session session) {
    jsonParser = fhirContext.newJsonParser();
    this.session = session;
  }

  private String buildSelectClause(List<DimensionAttribute> dimensionAttributes,
      List<Metric> metrics) {
    // TODO: Specify table name along with each dimension attribute name, to cater for situations where there are
    // duplicate dimension attribute names.
    String clause = "SELECT ";
    clause +=
        dimensionAttributes.stream()
            .map(Naming::fieldNameForDimensionAttribute)
            .collect(joining(", "));
    if (!dimensionAttributes.isEmpty()) {
      clause += ", ";
    }
    clause += metrics.stream().map(metric -> {
      String name = metric.getName();
      String expression = metric.getExpression();
      return expression + " AS " + quote(name);
    }).collect(joining(", "));
    return clause;
  }

  private String buildFromClause(List<DimensionAttribute> dimensionAttributes, List<Metric> metrics)
      throws ResourceNotFoundException {
    String clause = " FROM ";
    // TODO: We assume that all metrics within a Query are from the same FactSet, and that there is at least one
    // Metric in each Query. These facts will need to be validated somewhere.
    String factName = Naming.tableNameForFactSet(metrics.stream().findFirst().get().getFactSet());
    clause += factName;
    if (!dimensionAttributes.isEmpty()) {
      clause += " ";
      List<String> joins = new ArrayList<>();
      for (DimensionAttribute dimensionAttribute : dimensionAttributes) {
        String dimensionId = getIdFromReference(dimensionAttribute.getDimension());
        Dimension dimension = session.byId(Dimension.class).load(dimensionId);
        dimension.populateFromJson(jsonParser);
        String dimensionName = tableNameForDimension(dimension);
        String join = "LEFT OUTER JOIN ";
        join += dimensionName + " ON " + factName + ".key = " + dimensionName + ".key";
        joins.add(join);
      }
      clause += String.join(", ", joins);
    }
    return clause;
  }

  private String buildGroupByClause(List<DimensionAttribute> dimensionAttributes) {
    if (dimensionAttributes.isEmpty()) {
      return "";
    }
    String clause = " GROUP BY ";
    clause += dimensionAttributes.stream()
        .map(Naming::fieldNameForDimensionAttribute)
        .collect(joining(", "));
    return clause;
  }

  private String buildOrderByClause(List<DimensionAttribute> dimensionAttributes) {
    if (dimensionAttributes.isEmpty()) {
      return "";
    }
    String clause = " ORDER BY ";
    clause += dimensionAttributes.stream()
        .map(Naming::fieldNameForDimensionAttribute)
        .collect(joining(", "));
    return clause;
  }

  @Override
  public String translateQuery(Query query) throws ResourceNotFoundException {
    List<String> dimensionAttributeIds = query.getDimensionAttribute()
        .stream()
        .map(FhirUtilities::getIdFromReference)
        .collect(Collectors.toList());
    List<DimensionAttribute> dimensionAttributes = session.byMultipleIds(DimensionAttribute.class)
        .multiLoad(
            dimensionAttributeIds);
    int nullIndex = dimensionAttributes.indexOf(null);
    if (nullIndex != -1) {
      throw new ResourceNotFoundException(new IdDt(dimensionAttributeIds.get(nullIndex)));
    }
    dimensionAttributes.forEach(da -> da.populateFromJson(jsonParser));

    List<String> metricIds = query.getMetric()
        .stream()
        .map(FhirUtilities::getIdFromReference)
        .collect(Collectors.toList());
    List<Metric> metrics = session.byMultipleIds(Metric.class).multiLoad(metricIds);
    nullIndex = metrics.indexOf(null);
    if (nullIndex != -1) {
      throw new ResourceNotFoundException(new IdDt(metricIds.get(nullIndex)));
    }
    metrics.forEach(m -> m.populateFromJson(jsonParser));

    return buildSelectClause(dimensionAttributes, metrics)
        + buildFromClause(dimensionAttributes, metrics)
        + buildGroupByClause(dimensionAttributes)
        + buildOrderByClause(dimensionAttributes);
  }

}
