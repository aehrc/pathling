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
import org.hl7.fhir.dstu3.model.Reference;
import org.hl7.fhir.dstu3.model.Type;

/**
 * Describes the response to a request for aggregate statistics about data held within a FHIR
 * analytics server.
 *
 * @author John Grimes
 */
@ResourceDef(name = "AggregateQueryResult",
    profile = "https://clinsight.csiro.au/fhir/StructureDefinition/aggregate-query-result-0")
public class AggregateQueryResult extends Basic {

  @Child(name = "query")
  @Description(shortDefinition = "A reference to the AggregateQuery resource that this resource describes the results for")
  private Reference query;

  @Child(name = "grouping", max = MAX_UNLIMITED)
  private List<GroupingComponent> grouping;

  public Reference getQuery() {
    return query;
  }

  public void setQuery(Reference query) {
    this.query = query;
  }

  public List<GroupingComponent> getGrouping() {
    return grouping;
  }

  public void setGrouping(
      List<GroupingComponent> grouping) {
    this.grouping = grouping;
  }

  @Override
  protected void listChildren(List<Property> children) {
    super.listChildren(children);
    children.add(new Property("query",
        "Reference(AggregateQuery)",
        "A reference to the AggregateQuery resource that this resource describes the results for.",
        0,
        1,
        query));
    children.add(new Property("grouping",
        "BackboneElement",
        "The grouped results of the aggregations requested in the query.",
        0,
        MAX_UNLIMITED,
        grouping));
  }

  @Override
  public AggregateQueryResult copy() {
    AggregateQueryResult queryResult = new AggregateQueryResult();
    queryResult.query = query;
    queryResult.grouping = grouping;
    return queryResult;
  }

  @Override
  public void copyValues(DomainResource dst) {
    super.copyValues(dst);
    if (query != null) {
      ((AggregateQueryResult) dst).query = query.copy();
    }
    if (grouping != null) {
      ((AggregateQueryResult) dst).grouping = grouping.stream().map(GroupingComponent::copy)
          .collect(Collectors.toList());
    }
  }

  @Override
  public boolean isEmpty() {
    return super.isEmpty() && ElementUtil.isEmpty(query, grouping);
  }

  @Block
  public static class GroupingComponent extends BackboneElement {

    @Child(name = "label", max = MAX_UNLIMITED)
    private List<LabelComponent> label;

    @Child(name = "result", max = MAX_UNLIMITED)
    private List<ResultComponent> result;

    public List<LabelComponent> getLabel() {
      return label;
    }

    public void setLabel(List<LabelComponent> label) {
      this.label = label;
    }

    public List<ResultComponent> getResult() {
      return result;
    }

    public void setResult(List<ResultComponent> result) {
      this.result = result;
    }

    @Override
    protected void listChildren(List<Property> children) {
      super.listChildren(children);
      children.add(new Property("label",
          "BackboneElement",
          "The set of descriptive labels that describe this grouping, and correspond to those requested in the query.",
          0,
          MAX_UNLIMITED,
          label));
      children.add(new Property("result",
          "BackboneElement",
          "The set of values that resulted from the execution of the aggregations that were requested in the query.",
          0,
          MAX_UNLIMITED,
          result));
    }

    @Override
    public void copyValues(BackboneElement dst) {
      super.copyValues(dst);
      if (label != null) {
        ((GroupingComponent) dst).label = label.stream().map(LabelComponent::copy)
            .collect(Collectors.toList());
      }
      if (result != null) {
        ((GroupingComponent) dst).result = result.stream().map(ResultComponent::copy)
            .collect(Collectors.toList());
      }
    }

    @Override
    public boolean isEmpty() {
      return super.isEmpty() && ElementUtil.isEmpty(label, result);
    }

    @Override
    public GroupingComponent copy() {
      GroupingComponent grouping = new GroupingComponent();
      copyValues(grouping);
      return grouping;
    }
  }

  @Block
  public static class LabelComponent extends BackboneElement {

    @Child(name = "value")
    Type value;

    public Type getValue() {
      return value;
    }

    public void setValue(Type value) {
      this.value = value;
    }

    @Override
    protected void listChildren(List<Property> children) {
      super.listChildren(children);
      children.add(new Property("value",
          null,
          "The value for this label.",
          0,
          1,
          value));
    }

    @Override
    public void copyValues(BackboneElement dst) {
      super.copyValues(dst);
      if (value != null) {
        ((LabelComponent) dst).value = value.copy();
      }
    }

    @Override
    public boolean isEmpty() {
      return false;
    }

    @Override
    public LabelComponent copy() {
      LabelComponent label = new LabelComponent();
      copyValues(label);
      return label;
    }

  }

  @Block
  public static class ResultComponent extends BackboneElement {

    @Child(name = "value")
    Type value;

    public Type getValue() {
      return value;
    }

    public void setValue(Type value) {
      this.value = value;
    }

    @Override
    protected void listChildren(List<Property> children) {
      super.listChildren(children);
      children.add(new Property("value",
          null,
          "The value for this result.",
          0,
          1,
          value));
    }

    @Override
    public void copyValues(BackboneElement dst) {
      super.copyValues(dst);
      if (value != null) {
        ((ResultComponent) dst).value = value.copy();
      }
    }

    @Override
    public boolean isEmpty() {
      return false;
    }

    @Override
    public ResultComponent copy() {
      ResultComponent result = new ResultComponent();
      copyValues(result);
      return result;
    }

  }

}
