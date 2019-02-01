/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.resources;

import static ca.uhn.fhir.model.api.annotation.Child.MAX_UNLIMITED;

import ca.uhn.fhir.model.api.annotation.Block;
import ca.uhn.fhir.model.api.annotation.Child;
import ca.uhn.fhir.model.api.annotation.Description;
import ca.uhn.fhir.model.api.annotation.ResourceDef;
import ca.uhn.fhir.util.ElementUtil;
import java.util.List;
import java.util.stream.Collectors;
import org.hl7.fhir.dstu3.model.BackboneElement;
import org.hl7.fhir.dstu3.model.Basic;
import org.hl7.fhir.dstu3.model.DomainResource;
import org.hl7.fhir.dstu3.model.Property;
import org.hl7.fhir.dstu3.model.StringType;

/**
 * Describes a request for aggregate statistics about data held within a FHIR analytics server.
 *
 * @author John Grimes
 */
@ResourceDef(name = "AggregateQuery", profile = "https://clinsight.csiro.au/fhir/StructureDefinition/aggregate-query-0")
public class AggregateQuery extends Basic {

  @Child(name = "aggregation", min = 1, max = MAX_UNLIMITED)
  @Description(shortDefinition = "A description of a function which is used to calculate a summary value from each grouping of values")
  private List<AggregationComponent> aggregation;

  @Child(name = "grouping", max = MAX_UNLIMITED)
  @Description(shortDefinition = "A description of how to group additive data into groups or categories within the query result")
  private List<GroupingComponent> grouping;

  public List<AggregationComponent> getAggregation() {
    return aggregation;
  }

  public void setAggregation(
      List<AggregationComponent> aggregation) {
    this.aggregation = aggregation;
  }

  public List<GroupingComponent> getGrouping() {
    return grouping;
  }

  public void setGrouping(
      List<GroupingComponent> grouping) {
    this.grouping = grouping;
  }

  @Override
  public AggregateQuery copy() {
    AggregateQuery query = new AggregateQuery();
    copyValues(query);
    return query;
  }

  @Override
  public void copyValues(DomainResource dst) {
    super.copyValues(dst);
    if (aggregation != null) {
      ((AggregateQuery) dst).aggregation =
          aggregation.stream().map(AggregationComponent::copy).collect(Collectors.toList());
    }
    if (grouping != null) {
      ((AggregateQuery) dst).grouping = grouping.stream().map(GroupingComponent::copy)
          .collect(Collectors.toList());
    }
  }

  @Override
  protected void listChildren(List<Property> children) {
    super.listChildren(children);
    children.add(new Property("aggregation",
        "BackboneElement",
        "A description of a function which is used to calculate a summary value from each grouping of values.",
        1,
        MAX_UNLIMITED,
        aggregation));
    children.add(new Property("grouping",
        "BackboneElement",
        "A description of how to group additive data into groups or categories within the query result.",
        0,
        MAX_UNLIMITED,
        grouping));
  }

  @Override
  public boolean isEmpty() {
    return super.isEmpty() && ElementUtil.isEmpty(aggregation, grouping);
  }

  @Block
  public static class AggregationComponent extends BackboneElement {

    @Child(name = "expression", min = 1)
    @Description(shortDefinition = "A FHIRPath expression that defines the aggregation function")
    private StringType expression;

    @Child(name = "label")
    @Description(shortDefinition = "A short description for the aggregation, for display purposes")
    private StringType label;

    public StringType getExpression() {
      return expression;
    }

    public void setExpression(StringType expression) {
      this.expression = expression;
    }

    public StringType getLabel() {
      return label;
    }

    public void setLabel(StringType label) {
      this.label = label;
    }

    @Override
    protected void listChildren(List<Property> children) {
      super.listChildren(children);
      children.add(new Property("expression",
          "string",
          "A FHIRPath expression that defines the aggregation function.",
          1,
          1,
          expression));
      children.add(new Property("label",
          "string",
          "A short description for the aggregation, for display purposes.",
          0,
          1,
          label));
    }

    @Override
    public void copyValues(BackboneElement dst) {
      super.copyValues(dst);
      if (expression != null) {
        ((AggregationComponent) dst).expression = expression.copy();
      }
      if (label != null) {
        ((AggregationComponent) dst).label = label.copy();
      }
    }

    @Override
    public AggregationComponent copy() {
      AggregationComponent aggregation = new AggregationComponent();
      copyValues(aggregation);
      return aggregation;
    }

    @Override
    public boolean isEmpty() {
      return super.isEmpty() && ElementUtil.isEmpty(expression, label);
    }
  }

  @Block
  public static class GroupingComponent extends BackboneElement {

    @Child(name = "expression", min = 1)
    @Description(shortDefinition = "A FHIRPath expression that can be evaluated against each resource in the data set to determine a distinct set of categories")
    private StringType expression;

    @Child(name = "label")
    @Description(shortDefinition = "A short description for the grouping, for display purposes")
    private StringType label;

    public StringType getExpression() {
      return expression;
    }

    public void setExpression(StringType expression) {
      this.expression = expression;
    }

    public StringType getLabel() {
      return label;
    }

    public void setLabel(StringType label) {
      this.label = label;
    }

    @Override
    protected void listChildren(List<Property> children) {
      super.listChildren(children);
      children.add(new Property("expression",
          "string",
          "A FHIRPath expression that can be evaluated against each resource in the data set to determine a distinct set of categories.",
          1,
          1,
          expression));
      children.add(new Property("label",
          "string",
          "A short description for the grouping, for display purposes.",
          0,
          1,
          label));
    }

    @Override
    public void copyValues(BackboneElement dst) {
      super.copyValues(dst);
      if (expression != null) {
        ((GroupingComponent) dst).expression = expression.copy();
      }
      if (label != null) {
        ((GroupingComponent) dst).label = label.copy();
      }
    }

    @Override
    public GroupingComponent copy() {
      GroupingComponent grouping = new GroupingComponent();
      copyValues(grouping);
      return grouping;
    }

    @Override
    public boolean isEmpty() {
      return super.isEmpty() && ElementUtil.isEmpty(expression, label);
    }
  }
}
