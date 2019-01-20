/*
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight.persistence;

import static au.csiro.clinsight.persistence.Naming.generateRandomKey;
import static ca.uhn.fhir.model.api.annotation.Child.MAX_UNLIMITED;

import ca.uhn.fhir.model.api.annotation.Block;
import ca.uhn.fhir.model.api.annotation.Child;
import ca.uhn.fhir.model.api.annotation.ResourceDef;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.util.ElementUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.persistence.CollectionTable;
import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Embeddable;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Transient;
import org.hl7.fhir.dstu3.model.BackboneElement;
import org.hl7.fhir.dstu3.model.Bundle;
import org.hl7.fhir.dstu3.model.DomainResource;
import org.hl7.fhir.dstu3.model.IdType;
import org.hl7.fhir.dstu3.model.Property;
import org.hl7.fhir.dstu3.model.Reference;
import org.hl7.fhir.dstu3.model.ResourceType;
import org.hl7.fhir.dstu3.model.StringType;
import org.hl7.fhir.dstu3.model.UriType;

/**
 * Describes a dimension, which is a set of related data fields that are available for grouping and
 * filtering within a FHIR analytics server.
 *
 * @author John Grimes
 */
@Entity
@ResourceDef(name = "Dimension",
    profile = "https://clinsight.csiro.au/fhir/StructureDefinition/dimension-0")
public class Dimension extends DomainResource {

  public static final String URL =
      "https://clinsight.csiro.au/fhir/StructureDefinition/dimension-0";

  @Child(name = "name", min = 1)
  private StringType name;

  @Child(name = "title", min = 1)
  private StringType title;

  @Child(name = "attribute", max = MAX_UNLIMITED)
  private List<Reference> attribute;

  @Child(name = "describes", max = MAX_UNLIMITED)
  private List<DescribesComponent> describes;

  @Column(name = "json")
  private String json;

  public Dimension() {
    super();
    setKey(generateRandomKey());
    describes = new ArrayList<>();
  }

  public static Predicate<Bundle.BundleEntryComponent> isEntryDimension() {
    return entry -> entry.getResource()
        .getMeta()
        .getProfile()
        .stream()
        .anyMatch(profile -> profile.equals(URL));
  }

  @Id
  @org.hibernate.annotations.Type(type = "text")
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
  public List<Reference> getAttribute() {
    return attribute;
  }

  @Transient
  public void setAttribute(List<Reference> attribute) {
    this.attribute = attribute;
  }

  @ElementCollection
  @CollectionTable(indexes = {@Index(name = "codeSystem_url_idx", columnList = "url"),
      @Index(name = "valueSet_url_idx", columnList = "valueSet")})
  public List<DescribesComponent> getDescribes() {
    return describes;
  }

  public void setDescribes(List<DescribesComponent> describes) {
    this.describes = describes;
  }

  @org.hibernate.annotations.Type(type = "text")
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
    Dimension dimension = (Dimension) jsonParser.parseResource(json);
    dimension.copyValues(this);
  }

  @Transient
  @Override
  public DomainResource copy() {
    Dimension dimension = new Dimension();
    copyValues(dimension);
    return dimension;
  }

  @Transient
  @Override
  public void copyValues(DomainResource dst) {
    super.copyValues(dst);
    if (name != null) {
      ((Dimension) dst).name = name.copy();
    }
    if (title != null) {
      ((Dimension) dst).title = title.copy();
    }
    if (attribute != null) {
      ((Dimension) dst).attribute = attribute.stream().map(Reference::copy)
          .collect(Collectors.toList());
    }
    if (describes != null) {
      ((Dimension) dst).describes = describes.stream().map(DescribesComponent::copy)
          .collect(Collectors.toList());
    }
    if (json != null) {
      ((Dimension) dst).json = json;
    }
  }

  @Transient
  @Override
  public boolean isEmpty() {
    return super.isEmpty() && ElementUtil.isEmpty(name, title, attribute, describes);
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
    children.add(new Property("name", "string", "The display name of the Dimension.", 1, 1, name));
    children.add(
        new Property("title", "string", "The machine-friendly name of the Dimension.", 1, 1,
            title));
    children.add(new Property("attribute",
        "Reference(DimensionAttribute)",
        "The set of DimensionAttributes available within this Dimension.",
        0,
        MAX_UNLIMITED,
        attribute));
    children.add(new Property("describes",
        "BackboneElement",
        "A list of references to terminology resources or profiles that define the " +
            "structure of the data that is described by this Dimension.",
        0,
        1,
        describes));
  }

  @Embeddable
  @Block
  public static class DescribesComponent extends BackboneElement {

    @Child(name = "codeSystem")
    private DescribesCodeSystemComponent codeSystem;

    @Child(name = "valueSet")
    private UriType valueSet;

    @Child(name = "structureDefinition")
    private UriType structureDefinition;

    @Embedded
    public DescribesCodeSystemComponent getCodeSystem() {
      return codeSystem;
    }

    public void setCodeSystem(DescribesCodeSystemComponent codeSystem) {
      this.codeSystem = codeSystem;
    }

    @org.hibernate.annotations.Type(type = "text")
    public String getValueSet() {
      if (valueSet == null) {
        return null;
      }
      return valueSet.asStringValue();
    }

    public void setValueSet(String valueSet) {
      this.valueSet = new UriType(valueSet);
    }

    @org.hibernate.annotations.Type(type = "text")
    public String getStructureDefinition() {
      if (structureDefinition == null) {
        return null;
      }
      return structureDefinition.asStringValue();
    }

    public void setStructureDefinition(String structureDefinition) {
      this.structureDefinition = new UriType(structureDefinition);
    }

    @Transient
    @Override
    public DescribesComponent copy() {
      DescribesComponent describes = new DescribesComponent();
      copyValues(describes);
      return describes;
    }

    @Transient
    @Override
    public void copyValues(BackboneElement dst) {
      super.copyValues(dst);
      if (codeSystem != null) {
        ((DescribesComponent) dst).codeSystem = codeSystem.copy();
      }
      if (valueSet != null) {
        ((DescribesComponent) dst).valueSet = valueSet.copy();
      }
      if (structureDefinition != null) {
        ((DescribesComponent) dst).structureDefinition = structureDefinition.copy();
      }
    }

    @Transient
    @Override
    protected void listChildren(List<Property> children) {
      super.listChildren(children);
      children.add(new Property("codeSystem",
          "BackboneElement",
          "Reference to a CodeSystem that is described by this Dimension.",
          0,
          1,
          valueSet));
      children.add(new Property("valueSet",
          "uri",
          "Reference to a ValueSet that is described by this Dimension.",
          0,
          1,
          valueSet));
      children.add(new Property("structureDefinition",
          "uri",
          "Reference to a StructureDefinition that describes the data that is the subject" +
              " of this Dimension.",
          0,
          1,
          structureDefinition));
    }

    @Transient
    @Override
    public boolean isEmpty() {
      return super.isEmpty() && ElementUtil.isEmpty(codeSystem, valueSet, structureDefinition);
    }

  }

  @Embeddable
  @Block
  public static class DescribesCodeSystemComponent extends BackboneElement {

    @Child(name = "url", min = 1)
    private UriType url;

    @Child(name = "version")
    private StringType version;

    @org.hibernate.annotations.Type(type = "text")
    public String getUrl() {
      if (url == null) {
        return null;
      }
      return url.asStringValue();
    }

    public void setUrl(String url) {
      this.url = new UriType(url);
    }

    @org.hibernate.annotations.Type(type = "text")
    public String getVersion() {
      if (version == null) {
        return null;
      }
      return version.asStringValue();
    }

    public void setVersion(String version) {
      this.version = new StringType(version);
    }

    @Transient
    @Override
    public DescribesCodeSystemComponent copy() {
      DescribesCodeSystemComponent codeSystem = new DescribesCodeSystemComponent();
      copyValues(codeSystem);
      return codeSystem;
    }

    @Transient
    @Override
    public void copyValues(BackboneElement dst) {
      super.copyValues(dst);
      if (url != null) {
        ((DescribesCodeSystemComponent) dst).url = url.copy();
      }
      if (version != null) {
        ((DescribesCodeSystemComponent) dst).version = version.copy();
      }
    }

    @Transient
    @Override
    protected void listChildren(List<Property> children) {
      super.listChildren(children);
      children.add(new Property("url", "uri", "Logical URI of the code system.", 1, 1, url));
      children.add(new Property("version", "string", "Version of the code system.", 0, 1, version));
    }

    @Transient
    @Override
    public boolean isEmpty() {
      return super.isEmpty() && ElementUtil.isEmpty(url, version);
    }

  }

}
