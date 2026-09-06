/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2026 Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package au.csiro.pathling.encoders;

import ca.uhn.fhir.model.api.annotation.Block;
import ca.uhn.fhir.model.api.annotation.Child;
import ca.uhn.fhir.model.api.annotation.ResourceDef;
import jakarta.annotation.Nullable;
import java.io.Serial;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.hl7.fhir.r4.model.BackboneElement;
import org.hl7.fhir.r4.model.Base64BinaryType;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CanonicalType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.ContactDetail;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.DateType;
import org.hl7.fhir.r4.model.DecimalType;
import org.hl7.fhir.r4.model.DomainResource;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.MarkdownType;
import org.hl7.fhir.r4.model.OidType;
import org.hl7.fhir.r4.model.Period;
import org.hl7.fhir.r4.model.PositiveIntType;
import org.hl7.fhir.r4.model.RelatedArtifact;
import org.hl7.fhir.r4.model.ResourceType;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.TimeType;
import org.hl7.fhir.r4.model.Type;
import org.hl7.fhir.r4.model.UnsignedIntType;
import org.hl7.fhir.r4.model.UriType;
import org.hl7.fhir.r4.model.UrlType;
import org.hl7.fhir.r4.model.UsageContext;
import org.hl7.fhir.r4.model.UuidType;

/**
 * HAPI FHIR resource class for ViewDefinition from the SQL on FHIR specification.
 *
 * <p>This class allows HAPI to recognise and parse ViewDefinition resources. It mirrors the
 * structure of {@code au.csiro.pathling.views.FhirView} with HAPI annotations so that the JSON
 * structure is preserved during serialisation.
 *
 * <p>Every root element of ViewDefinition 3.0.0-ballot is declared, in the order given by the
 * StructureDefinition, which is also the order in which HAPI serialises them. The complex elements
 * use FHIR R4 datatypes, so sub-elements introduced in R5 or later are not retained: {@code
 * relatedArtifact.classifier}, {@code relatedArtifact.resourceReference}, {@code
 * relatedArtifact.publicationStatus}, {@code relatedArtifact.publicationDate}, the {@code height},
 * {@code width}, {@code frames}, {@code duration} and {@code pages} of {@code
 * relatedArtifact.document}, and {@code constant.valueInteger64}.
 *
 * @author John Grimes
 * @see <a
 *     href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition.html">ViewDefinition</a>
 */
@SuppressWarnings({"unused", "MissingJavadocMethod", "checkstyle:MissingJavadocMethod"})
@Setter
@ResourceDef(
    name = "ViewDefinition",
    profile = "http://hl7.org/fhir/uv/sql-on-fhir/StructureDefinition/ViewDefinition")
public class ViewDefinitionResource extends DomainResource {

  @Serial private static final long serialVersionUID = 1909997123685548098L;

  @Nullable
  @Child(name = "url")
  private UriType url;

  @Child(name = "identifier", max = Child.MAX_UNLIMITED)
  private List<Identifier> identifier;

  @Nullable
  @Child(name = "version")
  private StringType version;

  @Nullable
  @Getter
  @Child(
      name = "versionAlgorithm",
      type = {StringType.class, Coding.class})
  private Type versionAlgorithm;

  @Nullable
  @Getter
  @Child(name = "name")
  private StringType name;

  @Nullable
  @Child(name = "title")
  private StringType title;

  @Nullable
  @Getter
  @Child(name = "status")
  private CodeType status;

  @Nullable
  @Child(name = "experimental")
  private BooleanType experimental;

  @Nullable
  @Child(name = "date")
  private DateTimeType date;

  @Nullable
  @Child(name = "publisher")
  private StringType publisher;

  @Child(name = "contact", max = Child.MAX_UNLIMITED)
  private List<ContactDetail> contact;

  @Nullable
  @Child(name = "description")
  private MarkdownType description;

  @Child(name = "useContext", max = Child.MAX_UNLIMITED)
  private List<UsageContext> useContext;

  @Child(name = "jurisdiction", max = Child.MAX_UNLIMITED)
  private List<CodeableConcept> jurisdiction;

  @Nullable
  @Child(name = "purpose")
  private MarkdownType purpose;

  @Nullable
  @Child(name = "copyright")
  private MarkdownType copyright;

  @Nullable
  @Child(name = "copyrightLabel")
  private StringType copyrightLabel;

  @Nullable
  @Child(name = "approvalDate")
  private DateType approvalDate;

  @Nullable
  @Child(name = "lastReviewDate")
  private DateType lastReviewDate;

  @Nullable
  @Getter
  @Child(name = "effectivePeriod")
  private Period effectivePeriod;

  @Child(name = "topic", max = Child.MAX_UNLIMITED)
  private List<CodeableConcept> topic;

  @Child(name = "author", max = Child.MAX_UNLIMITED)
  private List<ContactDetail> author;

  @Child(name = "editor", max = Child.MAX_UNLIMITED)
  private List<ContactDetail> editor;

  @Child(name = "reviewer", max = Child.MAX_UNLIMITED)
  private List<ContactDetail> reviewer;

  @Child(name = "endorser", max = Child.MAX_UNLIMITED)
  private List<ContactDetail> endorser;

  @Child(name = "relatedArtifact", max = Child.MAX_UNLIMITED)
  private List<RelatedArtifact> relatedArtifact;

  @Nullable
  @Getter
  @Child(name = "resource", min = 1)
  private CodeType resource;

  @Child(name = "profile", max = Child.MAX_UNLIMITED)
  private List<CanonicalType> profile;

  @Child(name = "fhirVersion", max = Child.MAX_UNLIMITED)
  private List<CodeType> fhirVersion;

  @Child(name = "constant", max = Child.MAX_UNLIMITED)
  private List<ConstantComponent> constant;

  @Child(name = "select", min = 1, max = Child.MAX_UNLIMITED)
  private List<SelectComponent> select;

  @Child(name = "where", max = Child.MAX_UNLIMITED)
  private List<WhereComponent> where;

  @Nullable
  public String getUrl() {
    return url == null ? null : url.getValue();
  }

  @Nullable
  public UriType getUrlElement() {
    return url;
  }

  public boolean hasUrlElement() {
    return url != null && !url.isEmpty();
  }

  public void setUrlElement(final UriType url) {
    this.url = url;
  }

  public void setUrl(final String url) {
    this.url = url == null ? null : new UriType(url);
  }

  public List<Identifier> getIdentifier() {
    if (identifier == null) {
      identifier = new ArrayList<>();
    }
    return identifier;
  }

  public boolean hasIdentifier() {
    return identifier != null && !identifier.isEmpty();
  }

  @Nullable
  public String getVersion() {
    return version == null ? null : version.getValue();
  }

  @Nullable
  public StringType getVersionElement() {
    return version;
  }

  public boolean hasVersionElement() {
    return version != null && !version.isEmpty();
  }

  public void setVersionElement(final StringType version) {
    this.version = version;
  }

  public void setVersion(final String version) {
    this.version = version == null ? null : new StringType(version);
  }

  public boolean hasVersionAlgorithm() {
    return versionAlgorithm != null && !versionAlgorithm.isEmpty();
  }

  @Nullable
  public StringType getNameElement() {
    return name;
  }

  public boolean hasNameElement() {
    return name != null && !name.isEmpty();
  }

  public void setNameElement(final StringType name) {
    this.name = name;
  }

  @Nullable
  public StringType getTitleElement() {
    return title;
  }

  public boolean hasTitleElement() {
    return title != null && !title.isEmpty();
  }

  public void setTitleElement(final StringType title) {
    this.title = title;
  }

  @Nullable
  public CodeType getStatusElement() {
    return status;
  }

  public boolean hasStatusElement() {
    return status != null && !status.isEmpty();
  }

  public void setStatusElement(final CodeType status) {
    this.status = status;
  }

  @Nullable
  public BooleanType getExperimentalElement() {
    return experimental;
  }

  public boolean hasExperimentalElement() {
    return experimental != null && !experimental.isEmpty();
  }

  public void setExperimentalElement(final BooleanType experimental) {
    this.experimental = experimental;
  }

  @Nullable
  public DateTimeType getDateElement() {
    return date;
  }

  public boolean hasDateElement() {
    return date != null && !date.isEmpty();
  }

  public void setDateElement(final DateTimeType date) {
    this.date = date;
  }

  @Nullable
  public StringType getPublisherElement() {
    return publisher;
  }

  public boolean hasPublisherElement() {
    return publisher != null && !publisher.isEmpty();
  }

  public void setPublisherElement(final StringType publisher) {
    this.publisher = publisher;
  }

  public List<ContactDetail> getContact() {
    if (contact == null) {
      contact = new ArrayList<>();
    }
    return contact;
  }

  public boolean hasContact() {
    return contact != null && !contact.isEmpty();
  }

  @Nullable
  public MarkdownType getDescriptionElement() {
    return description;
  }

  public boolean hasDescriptionElement() {
    return description != null && !description.isEmpty();
  }

  public void setDescriptionElement(final MarkdownType description) {
    this.description = description;
  }

  public List<UsageContext> getUseContext() {
    if (useContext == null) {
      useContext = new ArrayList<>();
    }
    return useContext;
  }

  public boolean hasUseContext() {
    return useContext != null && !useContext.isEmpty();
  }

  public List<CodeableConcept> getJurisdiction() {
    if (jurisdiction == null) {
      jurisdiction = new ArrayList<>();
    }
    return jurisdiction;
  }

  public boolean hasJurisdiction() {
    return jurisdiction != null && !jurisdiction.isEmpty();
  }

  @Nullable
  public MarkdownType getPurposeElement() {
    return purpose;
  }

  public boolean hasPurposeElement() {
    return purpose != null && !purpose.isEmpty();
  }

  public void setPurposeElement(final MarkdownType purpose) {
    this.purpose = purpose;
  }

  @Nullable
  public MarkdownType getCopyrightElement() {
    return copyright;
  }

  public boolean hasCopyrightElement() {
    return copyright != null && !copyright.isEmpty();
  }

  public void setCopyrightElement(final MarkdownType copyright) {
    this.copyright = copyright;
  }

  @Nullable
  public StringType getCopyrightLabelElement() {
    return copyrightLabel;
  }

  public boolean hasCopyrightLabelElement() {
    return copyrightLabel != null && !copyrightLabel.isEmpty();
  }

  public void setCopyrightLabelElement(final StringType copyrightLabel) {
    this.copyrightLabel = copyrightLabel;
  }

  @Nullable
  public DateType getApprovalDateElement() {
    return approvalDate;
  }

  public boolean hasApprovalDateElement() {
    return approvalDate != null && !approvalDate.isEmpty();
  }

  public void setApprovalDateElement(final DateType approvalDate) {
    this.approvalDate = approvalDate;
  }

  @Nullable
  public DateType getLastReviewDateElement() {
    return lastReviewDate;
  }

  public boolean hasLastReviewDateElement() {
    return lastReviewDate != null && !lastReviewDate.isEmpty();
  }

  public void setLastReviewDateElement(final DateType lastReviewDate) {
    this.lastReviewDate = lastReviewDate;
  }

  public boolean hasEffectivePeriod() {
    return effectivePeriod != null && !effectivePeriod.isEmpty();
  }

  public List<CodeableConcept> getTopic() {
    if (topic == null) {
      topic = new ArrayList<>();
    }
    return topic;
  }

  public boolean hasTopic() {
    return topic != null && !topic.isEmpty();
  }

  public List<ContactDetail> getAuthor() {
    if (author == null) {
      author = new ArrayList<>();
    }
    return author;
  }

  public boolean hasAuthor() {
    return author != null && !author.isEmpty();
  }

  public List<ContactDetail> getEditor() {
    if (editor == null) {
      editor = new ArrayList<>();
    }
    return editor;
  }

  public boolean hasEditor() {
    return editor != null && !editor.isEmpty();
  }

  public List<ContactDetail> getReviewer() {
    if (reviewer == null) {
      reviewer = new ArrayList<>();
    }
    return reviewer;
  }

  public boolean hasReviewer() {
    return reviewer != null && !reviewer.isEmpty();
  }

  public List<ContactDetail> getEndorser() {
    if (endorser == null) {
      endorser = new ArrayList<>();
    }
    return endorser;
  }

  public boolean hasEndorser() {
    return endorser != null && !endorser.isEmpty();
  }

  public List<RelatedArtifact> getRelatedArtifact() {
    if (relatedArtifact == null) {
      relatedArtifact = new ArrayList<>();
    }
    return relatedArtifact;
  }

  public boolean hasRelatedArtifact() {
    return relatedArtifact != null && !relatedArtifact.isEmpty();
  }

  @Nullable
  public CodeType getResourceElement() {
    return resource;
  }

  public boolean hasResourceElement() {
    return resource != null && !resource.isEmpty();
  }

  public void setResourceElement(final CodeType resource) {
    this.resource = resource;
  }

  public List<CanonicalType> getProfile() {
    if (profile == null) {
      profile = new ArrayList<>();
    }
    return profile;
  }

  public boolean hasProfile() {
    return profile != null && !profile.isEmpty();
  }

  public List<CodeType> getFhirVersion() {
    if (fhirVersion == null) {
      fhirVersion = new ArrayList<>();
    }
    return fhirVersion;
  }

  public boolean hasFhirVersion() {
    return fhirVersion != null && !fhirVersion.isEmpty();
  }

  public List<ConstantComponent> getConstant() {
    if (constant == null) {
      constant = new ArrayList<>();
    }
    return constant;
  }

  public boolean hasConstant() {
    return constant != null && !constant.isEmpty();
  }

  public List<SelectComponent> getSelect() {
    if (select == null) {
      select = new ArrayList<>();
    }
    return select;
  }

  public boolean hasSelect() {
    return select != null && !select.isEmpty();
  }

  public List<WhereComponent> getWhere() {
    if (where == null) {
      where = new ArrayList<>();
    }
    return where;
  }

  public boolean hasWhere() {
    return where != null && !where.isEmpty();
  }

  @Override
  public DomainResource copy() {
    final ViewDefinitionResource copy = new ViewDefinitionResource();
    copyValues(copy);
    copy.url = url != null ? url.copy() : null;
    if (identifier != null) {
      copy.identifier = new ArrayList<>();
      for (final Identifier i : identifier) {
        copy.identifier.add(i.copy());
      }
    }
    copy.version = version != null ? version.copy() : null;
    copy.versionAlgorithm = versionAlgorithm != null ? versionAlgorithm.copy() : null;
    copy.name = name != null ? name.copy() : null;
    copy.title = title != null ? title.copy() : null;
    copy.status = status != null ? status.copy() : null;
    copy.experimental = experimental != null ? experimental.copy() : null;
    copy.date = date != null ? date.copy() : null;
    copy.publisher = publisher != null ? publisher.copy() : null;
    if (contact != null) {
      copy.contact = new ArrayList<>();
      for (final ContactDetail c : contact) {
        copy.contact.add(c.copy());
      }
    }
    copy.description = description != null ? description.copy() : null;
    if (useContext != null) {
      copy.useContext = new ArrayList<>();
      for (final UsageContext u : useContext) {
        copy.useContext.add(u.copy());
      }
    }
    if (jurisdiction != null) {
      copy.jurisdiction = new ArrayList<>();
      for (final CodeableConcept j : jurisdiction) {
        copy.jurisdiction.add(j.copy());
      }
    }
    copy.purpose = purpose != null ? purpose.copy() : null;
    copy.copyright = copyright != null ? copyright.copy() : null;
    copy.copyrightLabel = copyrightLabel != null ? copyrightLabel.copy() : null;
    copy.approvalDate = approvalDate != null ? approvalDate.copy() : null;
    copy.lastReviewDate = lastReviewDate != null ? lastReviewDate.copy() : null;
    copy.effectivePeriod = effectivePeriod != null ? effectivePeriod.copy() : null;
    if (topic != null) {
      copy.topic = new ArrayList<>();
      for (final CodeableConcept t : topic) {
        copy.topic.add(t.copy());
      }
    }
    if (author != null) {
      copy.author = new ArrayList<>();
      for (final ContactDetail a : author) {
        copy.author.add(a.copy());
      }
    }
    if (editor != null) {
      copy.editor = new ArrayList<>();
      for (final ContactDetail e : editor) {
        copy.editor.add(e.copy());
      }
    }
    if (reviewer != null) {
      copy.reviewer = new ArrayList<>();
      for (final ContactDetail r : reviewer) {
        copy.reviewer.add(r.copy());
      }
    }
    if (endorser != null) {
      copy.endorser = new ArrayList<>();
      for (final ContactDetail e : endorser) {
        copy.endorser.add(e.copy());
      }
    }
    if (relatedArtifact != null) {
      copy.relatedArtifact = new ArrayList<>();
      for (final RelatedArtifact r : relatedArtifact) {
        copy.relatedArtifact.add(r.copy());
      }
    }
    copy.resource = resource != null ? resource.copy() : null;
    if (profile != null) {
      copy.profile = new ArrayList<>();
      for (final CanonicalType p : profile) {
        copy.profile.add(p.copy());
      }
    }
    if (fhirVersion != null) {
      copy.fhirVersion = new ArrayList<>();
      for (final CodeType v : fhirVersion) {
        copy.fhirVersion.add(v.copy());
      }
    }
    if (constant != null) {
      copy.constant = new ArrayList<>();
      for (final ConstantComponent c : constant) {
        copy.constant.add(c.copy());
      }
    }
    if (select != null) {
      copy.select = new ArrayList<>();
      for (final SelectComponent s : select) {
        copy.select.add(s.copy());
      }
    }
    if (where != null) {
      copy.where = new ArrayList<>();
      for (final WhereComponent w : where) {
        copy.where.add(w.copy());
      }
    }
    return copy;
  }

  @Nullable
  @Override
  public ResourceType getResourceType() {
    // Custom resource types return null.
    return null;
  }

  @Override
  public String fhirType() {
    // Override to return the correct resource type name for this custom resource.
    return "ViewDefinition";
  }

  @Override
  public boolean isEmpty() {
    return super.isEmpty()
        && (url == null || url.isEmpty())
        && (identifier == null || identifier.isEmpty())
        && (version == null || version.isEmpty())
        && (versionAlgorithm == null || versionAlgorithm.isEmpty())
        && (name == null || name.isEmpty())
        && (title == null || title.isEmpty())
        && (status == null || status.isEmpty())
        && (experimental == null || experimental.isEmpty())
        && (date == null || date.isEmpty())
        && (publisher == null || publisher.isEmpty())
        && (contact == null || contact.isEmpty())
        && (description == null || description.isEmpty())
        && (useContext == null || useContext.isEmpty())
        && (jurisdiction == null || jurisdiction.isEmpty())
        && (purpose == null || purpose.isEmpty())
        && (copyright == null || copyright.isEmpty())
        && (copyrightLabel == null || copyrightLabel.isEmpty())
        && (approvalDate == null || approvalDate.isEmpty())
        && (lastReviewDate == null || lastReviewDate.isEmpty())
        && (effectivePeriod == null || effectivePeriod.isEmpty())
        && (topic == null || topic.isEmpty())
        && (author == null || author.isEmpty())
        && (editor == null || editor.isEmpty())
        && (reviewer == null || reviewer.isEmpty())
        && (endorser == null || endorser.isEmpty())
        && (relatedArtifact == null || relatedArtifact.isEmpty())
        && (resource == null || resource.isEmpty())
        && (profile == null || profile.isEmpty())
        && (fhirVersion == null || fhirVersion.isEmpty())
        && (constant == null || constant.isEmpty())
        && (select == null || select.isEmpty())
        && (where == null || where.isEmpty());
  }

  /** Select clause component. */
  @Block
  public static class SelectComponent extends BackboneElement {

    @Serial private static final long serialVersionUID = -52548946806162724L;

    @Setter
    @Child(name = "column", max = Child.MAX_UNLIMITED)
    private List<ColumnComponent> column;

    @Setter
    @Child(name = "select", max = Child.MAX_UNLIMITED)
    private List<SelectComponent> select;

    @Setter
    @Nullable
    @Getter
    @Child(name = "forEach")
    private StringType forEach;

    @Nullable
    @Getter
    @Child(name = "forEachOrNull")
    private StringType forEachOrNull;

    @Setter
    @Child(name = "unionAll", max = Child.MAX_UNLIMITED)
    private List<SelectComponent> unionAll;

    @Setter
    @Child(name = "repeat", max = Child.MAX_UNLIMITED)
    private List<StringType> repeat;

    public List<ColumnComponent> getColumn() {
      if (column == null) {
        column = new ArrayList<>();
      }
      return column;
    }

    public boolean hasColumn() {
      return column != null && !column.isEmpty();
    }

    public List<SelectComponent> getSelect() {
      if (select == null) {
        select = new ArrayList<>();
      }
      return select;
    }

    public boolean hasSelect() {
      return select != null && !select.isEmpty();
    }

    @Nullable
    public StringType getForEachElement() {
      return forEach;
    }

    public boolean hasForEachElement() {
      return forEach != null && !forEach.isEmpty();
    }

    public void setForEachElement(final StringType forEach) {
      this.forEach = forEach;
    }

    @Nullable
    public StringType getForEachOrNullElement() {
      return forEachOrNull;
    }

    public boolean hasForEachOrNullElement() {
      return forEachOrNull != null && !forEachOrNull.isEmpty();
    }

    public void setForEachOrNullElement(final StringType forEachOrNull) {
      this.forEachOrNull = forEachOrNull;
    }

    public void setForEachOrNull(final StringType forEachOrNull) {
      setForEachOrNullElement(forEachOrNull);
    }

    public List<SelectComponent> getUnionAll() {
      if (unionAll == null) {
        unionAll = new ArrayList<>();
      }
      return unionAll;
    }

    public boolean hasUnionAll() {
      return unionAll != null && !unionAll.isEmpty();
    }

    public List<StringType> getRepeat() {
      if (repeat == null) {
        repeat = new ArrayList<>();
      }
      return repeat;
    }

    public boolean hasRepeat() {
      return repeat != null && !repeat.isEmpty();
    }

    @Override
    public SelectComponent copy() {
      final SelectComponent copy = new SelectComponent();
      copyValues(copy);
      if (column != null) {
        copy.column = new ArrayList<>();
        for (final ColumnComponent c : column) {
          copy.column.add(c.copy());
        }
      }
      if (select != null) {
        copy.select = new ArrayList<>();
        for (final SelectComponent s : select) {
          copy.select.add(s.copy());
        }
      }
      copy.forEach = forEach != null ? forEach.copy() : null;
      copy.forEachOrNull = forEachOrNull != null ? forEachOrNull.copy() : null;
      if (unionAll != null) {
        copy.unionAll = new ArrayList<>();
        for (final SelectComponent u : unionAll) {
          copy.unionAll.add(u.copy());
        }
      }
      if (repeat != null) {
        copy.repeat = new ArrayList<>();
        for (final StringType r : repeat) {
          copy.repeat.add(r.copy());
        }
      }
      return copy;
    }

    @Override
    public boolean isEmpty() {
      return super.isEmpty()
          && (column == null || column.isEmpty())
          && (select == null || select.isEmpty())
          && forEach == null
          && forEachOrNull == null
          && (unionAll == null || unionAll.isEmpty())
          && (repeat == null || repeat.isEmpty());
    }
  }

  /** Column component. */
  @Block
  public static class ColumnComponent extends BackboneElement {

    @Serial private static final long serialVersionUID = -4337858165238555555L;

    @Nullable
    @Getter
    @Child(name = "name", min = 1)
    private StringType name;

    @Nullable
    @Setter
    @Getter
    @Child(name = "path", min = 1)
    private StringType path;

    @Nullable
    @Setter
    @Getter
    @Child(name = "description")
    private StringType description;

    @Nullable
    @Setter
    @Getter
    @Child(name = "collection")
    private BooleanType collection;

    @Nullable
    @Setter
    @Getter
    @Child(name = "type")
    private StringType type;

    @Setter
    @Child(name = "tag", max = Child.MAX_UNLIMITED)
    private List<TagComponent> tag;

    @Nullable
    public StringType getNameElement() {
      return name;
    }

    public boolean hasNameElement() {
      return name != null && !name.isEmpty();
    }

    public void setNameElement(final StringType name) {
      this.name = name;
    }

    public void setName(final StringType name) {
      setNameElement(name);
    }

    @Nullable
    public StringType getPathElement() {
      return path;
    }

    public boolean hasPathElement() {
      return path != null && !path.isEmpty();
    }

    public void setPathElement(final StringType path) {
      this.path = path;
    }

    @Nullable
    public StringType getDescriptionElement() {
      return description;
    }

    public boolean hasDescriptionElement() {
      return description != null && !description.isEmpty();
    }

    public void setDescriptionElement(final StringType description) {
      this.description = description;
    }

    @Nullable
    public BooleanType getCollectionElement() {
      return collection;
    }

    public boolean hasCollectionElement() {
      return collection != null && !collection.isEmpty();
    }

    public void setCollectionElement(final BooleanType collection) {
      this.collection = collection;
    }

    @Nullable
    public StringType getTypeElement() {
      return type;
    }

    public boolean hasTypeElement() {
      return type != null && !type.isEmpty();
    }

    public void setTypeElement(final StringType type) {
      this.type = type;
    }

    public List<TagComponent> getTag() {
      if (tag == null) {
        tag = new ArrayList<>();
      }
      return tag;
    }

    public boolean hasTag() {
      return tag != null && !tag.isEmpty();
    }

    @Override
    public ColumnComponent copy() {
      final ColumnComponent copy = new ColumnComponent();
      copyValues(copy);
      copy.name = name != null ? name.copy() : null;
      copy.path = path != null ? path.copy() : null;
      copy.description = description != null ? description.copy() : null;
      copy.collection = collection != null ? collection.copy() : null;
      copy.type = type != null ? type.copy() : null;
      if (tag != null) {
        copy.tag = new ArrayList<>();
        for (final TagComponent t : tag) {
          copy.tag.add(t.copy());
        }
      }
      return copy;
    }

    @Override
    public boolean isEmpty() {
      return super.isEmpty()
          && name == null
          && path == null
          && description == null
          && collection == null
          && type == null
          && (tag == null || tag.isEmpty());
    }
  }

  /** Tag component for columns. */
  @Setter
  @Getter
  @Block
  public static class TagComponent extends BackboneElement {

    @Serial private static final long serialVersionUID = 7134093987297739952L;

    @Nullable
    @Child(name = "name", min = 1)
    private StringType name;

    @Nullable
    @Child(name = "value")
    private StringType value;

    @Nullable
    public StringType getNameElement() {
      return name;
    }

    public boolean hasNameElement() {
      return name != null && !name.isEmpty();
    }

    public void setNameElement(final StringType name) {
      this.name = name;
    }

    @Nullable
    public StringType getValueElement() {
      return value;
    }

    public boolean hasValueElement() {
      return value != null && !value.isEmpty();
    }

    public void setValueElement(final StringType value) {
      this.value = value;
    }

    @Override
    public TagComponent copy() {
      final TagComponent copy = new TagComponent();
      copyValues(copy);
      copy.name = name != null ? name.copy() : null;
      copy.value = value != null ? value.copy() : null;
      return copy;
    }

    @Override
    public boolean isEmpty() {
      return super.isEmpty() && name == null && value == null;
    }
  }

  /** Where clause component. */
  @Block
  public static class WhereComponent extends BackboneElement {

    @Serial private static final long serialVersionUID = -3113017430382830946L;

    @Nullable
    @Child(name = "path", min = 1)
    private StringType path;

    @Nullable
    @Setter
    @Getter
    @Child(name = "description")
    private StringType description;

    @Nullable
    public StringType getPathElement() {
      return path;
    }

    public boolean hasPathElement() {
      return path != null && !path.isEmpty();
    }

    public void setPathElement(final StringType path) {
      this.path = path;
    }

    @Nullable
    public StringType getPath() {
      return getPathElement();
    }

    public void setPath(final StringType path) {
      setPathElement(path);
    }

    @Nullable
    public StringType getDescriptionElement() {
      return description;
    }

    public boolean hasDescriptionElement() {
      return description != null && !description.isEmpty();
    }

    public void setDescriptionElement(final StringType description) {
      this.description = description;
    }

    @Override
    public WhereComponent copy() {
      final WhereComponent copy = new WhereComponent();
      copyValues(copy);
      copy.path = path != null ? path.copy() : null;
      copy.description = description != null ? description.copy() : null;
      return copy;
    }

    @Override
    public boolean isEmpty() {
      return super.isEmpty() && path == null && description == null;
    }
  }

  /** Constant component. */
  @Block
  public static class ConstantComponent extends BackboneElement {

    @Serial private static final long serialVersionUID = -8087206196316257611L;

    @Nullable
    @Child(name = "name", min = 1)
    private StringType name;

    @Nullable
    @Setter
    @Getter
    @Child(
        name = "value",
        type = {
          Base64BinaryType.class,
          BooleanType.class,
          CanonicalType.class,
          CodeType.class,
          DateType.class,
          DateTimeType.class,
          DecimalType.class,
          IdType.class,
          InstantType.class,
          IntegerType.class,
          OidType.class,
          PositiveIntType.class,
          StringType.class,
          TimeType.class,
          UnsignedIntType.class,
          UriType.class,
          UrlType.class,
          UuidType.class
        })
    private org.hl7.fhir.r4.model.Type value;

    @Nullable
    public StringType getNameElement() {
      return name;
    }

    public boolean hasNameElement() {
      return name != null && !name.isEmpty();
    }

    public void setNameElement(final StringType name) {
      this.name = name;
    }

    @Nullable
    public StringType getName() {
      return getNameElement();
    }

    public void setName(final StringType name) {
      setNameElement(name);
    }

    public boolean hasValue() {
      return value != null && !value.isEmpty();
    }

    @Override
    public ConstantComponent copy() {
      final ConstantComponent copy = new ConstantComponent();
      copyValues(copy);
      copy.name = name != null ? name.copy() : null;
      copy.value = value != null ? value.copy() : null;
      return copy;
    }

    @Override
    public boolean isEmpty() {
      return super.isEmpty() && name == null && value == null;
    }
  }
}
