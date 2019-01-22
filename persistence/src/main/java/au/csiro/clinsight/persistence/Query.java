/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.persistence;

import static ca.uhn.fhir.model.api.annotation.Child.MAX_UNLIMITED;

import ca.uhn.fhir.model.api.annotation.Child;
import ca.uhn.fhir.model.api.annotation.ResourceDef;
import ca.uhn.fhir.util.ElementUtil;
import java.util.List;
import java.util.stream.Collectors;
import org.hl7.fhir.dstu3.model.DomainResource;
import org.hl7.fhir.dstu3.model.Property;
import org.hl7.fhir.dstu3.model.Reference;
import org.hl7.fhir.dstu3.model.ResourceType;

/**
 * Describes a request for aggregate statistics about data held within a FHIR analytics server.
 *
 * @author John Grimes
 */
@ResourceDef(name = "Query", profile = "https://clinsight.csiro.au/fhir/StructureDefinition/query-0")
public class Query extends DomainResource {

  @Child(name = "dimensionAttribute", max = MAX_UNLIMITED, order = 0)
  private List<Reference> dimensionAttribute;

  @Child(name = "metric", min = 1, max = MAX_UNLIMITED, order = 1)
  private List<Reference> metric;

  // TODO: Add filter functionality.

  public List<Reference> getDimensionAttribute() {
    return dimensionAttribute;
  }

  public void setDimensionAttribute(List<Reference> dimensionAttribute) {
    this.dimensionAttribute = dimensionAttribute;
  }

  public List<Reference> getMetric() {
    return metric;
  }

  public void setMetric(List<Reference> metric) {
    this.metric = metric;
  }

  @Override
  public DomainResource copy() {
    Query query = new Query();
    copyValues(query);
    return query;
  }

  @Override
  public void copyValues(DomainResource dst) {
    super.copyValues(dst);
    if (dimensionAttribute != null) {
      ((Query) dst).dimensionAttribute =
          dimensionAttribute.stream().map(Reference::copy).collect(Collectors.toList());
    }
    if (metric != null) {
      ((Query) dst).metric = metric.stream().map(Reference::copy).collect(Collectors.toList());
    }
  }

  @Override
  public ResourceType getResourceType() {
    return null;
  }

  @Override
  protected void listChildren(List<Property> children) {
    super.listChildren(children);
    children.add(new Property("dimensionAttribute",
        "Reference(DimensionAttribute)",
        "The set of DimensionAttributes that are be used to group the results of this query.",
        0,
        MAX_UNLIMITED,
        dimensionAttribute));
    children.add(new Property("metric",
        "Reference(Metric)",
        "The set of metrics that are to be used to aggregate the data.",
        0,
        MAX_UNLIMITED,
        metric));
  }

  @Override
  public boolean isEmpty() {
    return super.isEmpty() && ElementUtil.isEmpty(dimensionAttribute, metric);
  }

}
