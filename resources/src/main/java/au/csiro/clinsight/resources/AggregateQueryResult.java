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
import org.hl7.fhir.dstu3.model.StringType;
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

  @Child(name = "label", max = MAX_UNLIMITED)
  @Description(shortDefinition = "A set of display descriptions corresponding to the set of data items")
  private List<LabelComponent> label;

  @Child(name = "data", max = MAX_UNLIMITED)
  @Description(shortDefinition = "The calculated summary values that form the result of the execution of the query")
  private List<DataComponent> data;

  public Reference getQuery() {
    return query;
  }

  public void setQuery(Reference query) {
    this.query = query;
  }

  public List<LabelComponent> getLabel() {
    return label;
  }

  public void setLabel(List<LabelComponent> label) {
    this.label = label;
  }

  public List<DataComponent> getData() {
    return data;
  }

  public void setData(List<DataComponent> data) {
    this.data = data;
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
    children.add(new Property("label",
        "BackboneElement",
        "A set of display descriptions corresponding to the set of data items.",
        0,
        MAX_UNLIMITED,
        label));
    children.add(new Property("data",
        "BackboneElement",
        "The calculated summary values that form the result of the execution of the query.",
        0,
        MAX_UNLIMITED,
        data));
  }

  @Override
  public AggregateQueryResult copy() {
    AggregateQueryResult queryResult = new AggregateQueryResult();
    queryResult.query = query;
    queryResult.label = label;
    queryResult.data = data;
    return queryResult;
  }

  @Override
  public void copyValues(DomainResource dst) {
    super.copyValues(dst);
    if (query != null) {
      ((AggregateQueryResult) dst).query = query.copy();
    }
    if (label != null) {
      ((AggregateQueryResult) dst).label = label.stream().map(LabelComponent::copy)
          .collect(Collectors.toList());
    }
    if (data != null) {
      ((AggregateQueryResult) dst).data = data.stream().map(DataComponent::copy)
          .collect(Collectors.toList());
    }
  }

  @Override
  public boolean isEmpty() {
    return super.isEmpty() && ElementUtil.isEmpty(query, label, data);
  }

  @Block
  public static class LabelComponent extends BackboneElement {

    @Child(name = "name")
    @Description(shortDefinition = "A short description for this set of labels, for display purposes")
    private StringType name;

    @Child(name = "series", min = 1, max = MAX_UNLIMITED)
    @Description(shortDefinition = "The values for each label in the set")
    private List<StringType> series;

    public StringType getName() {
      return name;
    }

    public void setName(StringType name) {
      this.name = name;
    }

    public List<StringType> getSeries() {
      return series;
    }

    public void setSeries(List<StringType> series) {
      this.series = series;
    }

    @Override
    public LabelComponent copy() {
      LabelComponent labelComponent = new LabelComponent();
      copyValues(labelComponent);
      return labelComponent;
    }

    @Override
    public void copyValues(BackboneElement dst) {
      super.copyValues(dst);
      if (name != null) {
        ((LabelComponent) dst).name = name.copy();
      }
      if (series != null) {
        ((LabelComponent) dst).series = series.stream().map(StringType::copy)
            .collect(Collectors.toList());
      }
    }

    @Override
    protected void listChildren(List<Property> children) {
      super.listChildren(children);
      children.add(new Property("name",
          "string",
          "A short description for this set of labels, for display purposes",
          0,
          1,
          name));
      children.add(new Property("series",
          "Element",
          "The values for each label in the set",
          0,
          MAX_UNLIMITED,
          series));
    }

    @Override
    public boolean isEmpty() {
      return super.isEmpty() && ElementUtil.isEmpty(name, series);
    }

  }

  @Block
  public static class DataComponent extends BackboneElement {

    @Child(name = "name")
    @Description(shortDefinition = "A short description for this data series, for display purposes")
    private StringType name;

    @Child(name = "series", max = MAX_UNLIMITED)
    @Description(shortDefinition = "The values in the data series")
    private List<Type> series;

    public StringType getName() {
      return name;
    }

    public void setName(StringType name) {
      this.name = name;
    }

    public List<Type> getSeries() {
      return series;
    }

    public void setSeries(List<Type> series) {
      this.series = series;
    }

    @Override
    protected void listChildren(List<Property> children) {
      super.listChildren(children);
      children.add(new Property("name",
          "string",
          "A short description for this data series, for display purposes.",
          0,
          1,
          name));
      children.add(new Property("series",
          "Element",
          "The values in the data series",
          0,
          MAX_UNLIMITED,
          series));
    }

    @Override
    public DataComponent copy() {
      DataComponent dataComponent = new DataComponent();
      copyValues(dataComponent);
      return dataComponent;
    }

    @Override
    public void copyValues(BackboneElement dst) {
      super.copyValues(dst);
      if (name != null) {
        ((DataComponent) dst).name = name.copy();
      }
      if (series != null) {
        ((DataComponent) dst).series = series.stream().map(Type::copy).collect(Collectors.toList());
      }
    }

    @Override
    public boolean isEmpty() {
      return super.isEmpty() && ElementUtil.isEmpty(name, series);
    }

  }

}
