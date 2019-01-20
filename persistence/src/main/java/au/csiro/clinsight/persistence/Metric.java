/*
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight.persistence;

import static au.csiro.clinsight.persistence.Naming.generateRandomKey;

import ca.uhn.fhir.model.api.annotation.Child;
import ca.uhn.fhir.model.api.annotation.ResourceDef;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.util.ElementUtil;
import java.util.List;
import java.util.function.Predicate;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.persistence.UniqueConstraint;
import org.hl7.fhir.dstu3.model.Bundle;
import org.hl7.fhir.dstu3.model.DomainResource;
import org.hl7.fhir.dstu3.model.IdType;
import org.hl7.fhir.dstu3.model.Property;
import org.hl7.fhir.dstu3.model.ResourceType;
import org.hl7.fhir.dstu3.model.StringType;

/**
 * Describes a statistical measure over a set of facts. Metrics can be used in conjunction with
 * DimensionAttributes and Filters to request aggregate statistics about data held within a FHIR
 * analytics server.
 *
 * @author John Grimes
 */
@Entity
@ResourceDef(name = "Metric",
    profile = "https://clinsight.csiro.au/fhir/StructureDefinition/metric-0")
@Table(uniqueConstraints = @UniqueConstraint(name = "factSet_name_uniq", columnNames = {
    "factSet_key", "name"}))
public class Metric extends DomainResource {

  public static final String URL = "https://clinsight.csiro.au/fhir/StructureDefinition/metric-0";

  @Child(name = "name", min = 1)
  private StringType name;

  @Child(name = "title", min = 1)
  private StringType title;

  @Child(name = "expression")
  private StringType expression;

  @Column(name = "json")
  private String json;

  private FactSet factSet;

  public Metric() {
    setKey(generateRandomKey());
  }

  public static Predicate<Bundle.BundleEntryComponent> isEntryMetric() {
    return entry -> entry.getResource()
        .getMeta()
        .getProfile()
        .stream()
        .anyMatch(profile -> profile.equals(URL));
  }

  @Id
  public String getKey() {
    return getIdElement().getIdPart();
  }

  @Transient
  public void setKey(String key) {
    setIdElement(new IdType(key));
  }

  public String getName() {
    if (name == null) {
      return null;
    }
    return name.asStringValue();
  }

  public void setName(String name) {
    this.name = new StringType(name);
  }

  public String getTitle() {
    if (title == null) {
      return null;
    }
    return title.asStringValue();
  }

  public void setTitle(String title) {
    this.title = new StringType(title);
  }

  @Transient
  public String getExpression() {
    if (expression == null) {
      return null;
    }
    return expression.asStringValue();
  }

  @Transient
  public void setExpression(String expression) {
    this.expression = new StringType(expression);
  }

  @org.hibernate.annotations.Type(type = "text")
  public String getJson() {
    return json;
  }

  public void setJson(String json) {
    this.json = json;
  }

  @ManyToOne
  public FactSet getFactSet() {
    return factSet;
  }

  public void setFactSet(FactSet factSet) {
    this.factSet = factSet;
  }

  /**
   * Generate the JSON representation of this resource, using the supplied parser, and put it into
   * the `json` field, ready for persistence.
   *
   * @param jsonParser Parser to use for the JSON generation.
   */
  @Transient
  public void generateJson(IParser jsonParser) {
    json = jsonParser.encodeResourceToString(this);
  }

  /**
   * Parse the JSON resource within the `json` field, and populate its values into this resource.
   *
   * @param jsonParser Parser to use for de-serialising the JSON data.
   */
  @Transient
  public void populateFromJson(IParser jsonParser) {
    if (json == null) {
      return;
    }
    Metric metric = (Metric) jsonParser.parseResource(json);
    metric.copyValues(this);
  }

  @Transient
  @Override
  public DomainResource copy() {
    Metric metric = new Metric();
    copyValues(metric);
    return metric;
  }

  @Transient
  @Override
  public void copyValues(DomainResource dst) {
    super.copyValues(dst);
    if (name != null) {
      ((Metric) dst).name = name.copy();
    }
    if (title != null) {
      ((Metric) dst).title = title.copy();
    }
    if (expression != null) {
      ((Metric) dst).expression = expression.copy();
    }
    if (json != null) {
      ((Metric) dst).json = json;
    }
  }

  @Transient
  @Override
  public ResourceType getResourceType() {
    return null;
  }

  @Transient
  @Override
  protected void listChildren(List<Property> children) {
    super.listChildren(children);
    children.add(new Property("name", "string", "The display name of the Metric.", 1, 1, name));
    children.add(
        new Property("title", "string", "The machine-friendly name of the Metric.", 1, 1, title));
    children.add(new Property("expression",
        "string",
        "A SQL expression describing an aggregate function over the additive facts that are" +
            " the subject of this Metric.",
        0,
        1,
        expression));
  }

  @Transient
  @Override
  public boolean isEmpty() {
    return super.isEmpty() && ElementUtil.isEmpty(name, expression);
  }

}
