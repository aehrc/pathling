/*
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight.persistence;

import static au.csiro.clinsight.persistence.Naming.generateRandomKey;

import ca.uhn.fhir.model.api.annotation.Binding;
import ca.uhn.fhir.model.api.annotation.Child;
import ca.uhn.fhir.model.api.annotation.ResourceDef;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.util.ElementUtil;
import java.util.List;
import java.util.function.Predicate;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Transient;
import org.hibernate.annotations.Type;
import org.hl7.fhir.dstu3.model.Bundle;
import org.hl7.fhir.dstu3.model.Coding;
import org.hl7.fhir.dstu3.model.DomainResource;
import org.hl7.fhir.dstu3.model.IdType;
import org.hl7.fhir.dstu3.model.Property;
import org.hl7.fhir.dstu3.model.Reference;
import org.hl7.fhir.dstu3.model.ResourceType;
import org.hl7.fhir.dstu3.model.StringType;

/**
 * Describes a data field which is available for grouping and filtering within a FHIR analytics
 * server.
 *
 * @author John Grimes
 */
@Entity
@ResourceDef(name = "DimensionAttribute",
    profile = "https://clinsight.csiro.au/fhir/StructureDefinition/dimension-attribute-0")
public class DimensionAttribute extends DomainResource {

  public static final String URL =
      "https://clinsight.csiro.au/fhir/StructureDefinition/dimension-attribute-0";

  @Child(name = "name", min = 1)
  private StringType name;

  @Child(name = "title", min = 1)
  private StringType title;

  @Child(name = "type", min = 1)
  @Binding(valueSet = "http://hl7.org/fhir/ValueSet/defined-types")
  private Coding type;

  @Child(name = "dimension", min = 1)
  private Reference dimension;

  @Column(name = "json")
  private String json;

  public DimensionAttribute() {
    setKey(generateRandomKey());
  }

  public static Predicate<Bundle.BundleEntryComponent> isEntryDimensionAttribute() {
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

  public void setKey(String key) {
    setIdElement(new IdType(key));
  }

  @Transient
  public String getName() {
    if (name == null) {
      return null;
    }
    return name.asStringValue();
  }

  @Transient
  public void setName(String name) {
    this.name = new StringType(name);
  }

  @Transient
  public String getTitle() {
    if (title == null) {
      return null;
    }
    return title.asStringValue();
  }

  @Transient
  public void setTitle(String title) {
    this.title = new StringType(title);
  }

  @Transient
  public Coding getType() {
    return type;
  }

  @Transient
  public void setType(Coding type) {
    this.type = type;
  }

  @Transient
  public Reference getDimension() {
    return dimension;
  }

  @Transient
  public void setDimension(Reference dimension) {
    this.dimension = dimension;
  }

  @Type(type = "text")
  public String getJson() {
    return json;
  }

  public void setJson(String json) {
    this.json = json;
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
    DimensionAttribute dimensionAttribute = (DimensionAttribute) jsonParser.parseResource(json);
    dimensionAttribute.copyValues(this);
  }

  @Transient
  @Override
  public DomainResource copy() {
    DimensionAttribute dimensionAttribute = new DimensionAttribute();
    copyValues(dimensionAttribute);
    return dimensionAttribute;
  }

  @Transient
  @Override
  public void copyValues(DomainResource dst) {
    super.copyValues(dst);
    if (name != null) {
      ((DimensionAttribute) dst).name = name.copy();
    }
    if (title != null) {
      ((DimensionAttribute) dst).title = title.copy();
    }
    if (type != null) {
      ((DimensionAttribute) dst).type = type.copy();
    }
    if (dimension != null) {
      ((DimensionAttribute) dst).dimension = dimension.copy();
    }
  }

  @Transient
  @Override
  public ResourceType getResourceType() {
    return null;
  }

  @Override
  protected void listChildren(List<Property> children) {
    super.listChildren(children);
    children.add(
        new Property("name", "string", "The display name of the DimensionAttribute.", 1, 1, name));
    children.add(new Property("title",
        "string",
        "The machine-friendly name of the DimensionAttribute.",
        1,
        1,
        title));
    children.add(new Property("type",
        "Coding",
        "The data type used for representing values for this DimensionAttribute.",
        1,
        1,
        type));
    children.add(new Property("dimension",
        "Reference(Dimension)",
        "The Dimension that this DimensionAttribute forms a part of.",
        1,
        1,
        dimension));
  }

  @Transient
  @Override
  public boolean isEmpty() {
    return super.isEmpty() && ElementUtil.isEmpty(name, title, type, dimension);
  }

}
