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
 *
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
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.DateType;
import org.hl7.fhir.r4.model.DecimalType;
import org.hl7.fhir.r4.model.DomainResource;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.OidType;
import org.hl7.fhir.r4.model.PositiveIntType;
import org.hl7.fhir.r4.model.ResourceType;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.TimeType;
import org.hl7.fhir.r4.model.UnsignedIntType;
import org.hl7.fhir.r4.model.UriType;
import org.hl7.fhir.r4.model.UrlType;
import org.hl7.fhir.r4.model.UuidType;

/**
 * HAPI FHIR resource class for ViewDefinition from the SQL on FHIR specification.
 *
 * <p>This class allows HAPI to recognise and parse ViewDefinition resources. It mirrors the
 * structure of {@code au.csiro.pathling.views.FhirView} with HAPI annotations so that the JSON
 * structure is preserved during serialisation.
 *
 * @author John Grimes
 * @see <a
 *     href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition.html">ViewDefinition</a>
 */
@SuppressWarnings({"unused", "MissingJavadoc", "CheckStyle"})
@Setter
@ResourceDef(
    name = "ViewDefinition",
    profile = "http://hl7.org/fhir/uv/sql-on-fhir/StructureDefinition/ViewDefinition")
public class ViewDefinitionResource extends DomainResource {

  @Serial private static final long serialVersionUID = 1909997123685548098L;

  @Nullable
  @Getter
  @Child(name = "name")
  private StringType name;

  @Child(name = "fhirVersion", max = Child.MAX_UNLIMITED)
  private List<CodeType> fhirVersion;

  @Nullable
  @Getter
  @Child(name = "resource", min = 1)
  private CodeType resource;

  @Nullable
  @Getter
  @Child(name = "status")
  private CodeType status;

  @Child(name = "select", min = 1, max = Child.MAX_UNLIMITED)
  private List<SelectComponent> select;

  @Child(name = "where", max = Child.MAX_UNLIMITED)
  private List<WhereComponent> where;

  @Child(name = "constant", max = Child.MAX_UNLIMITED)
  private List<ConstantComponent> constant;

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

  public List<CodeType> getFhirVersion() {
    if (fhirVersion == null) {
      fhirVersion = new ArrayList<>();
    }
    return fhirVersion;
  }

  public boolean hasFhirVersion() {
    return fhirVersion != null && !fhirVersion.isEmpty();
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

  public List<ConstantComponent> getConstant() {
    if (constant == null) {
      constant = new ArrayList<>();
    }
    return constant;
  }

  public boolean hasConstant() {
    return constant != null && !constant.isEmpty();
  }

  @Override
  public DomainResource copy() {
    final ViewDefinitionResource copy = new ViewDefinitionResource();
    copyValues(copy);
    copy.name = name != null ? name.copy() : null;
    if (fhirVersion != null) {
      copy.fhirVersion = new ArrayList<>();
      for (final CodeType v : fhirVersion) {
        copy.fhirVersion.add(v.copy());
      }
    }
    copy.resource = resource != null ? resource.copy() : null;
    copy.status = status != null ? status.copy() : null;
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
    if (constant != null) {
      copy.constant = new ArrayList<>();
      for (final ConstantComponent c : constant) {
        copy.constant.add(c.copy());
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
        && (name == null || name.isEmpty())
        && (fhirVersion == null || fhirVersion.isEmpty())
        && (resource == null || resource.isEmpty())
        && (status == null || status.isEmpty())
        && (select == null || select.isEmpty())
        && (where == null || where.isEmpty())
        && (constant == null || constant.isEmpty());
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
