/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
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
import java.util.ArrayList;
import java.util.List;
import org.hl7.fhir.r4.model.BackboneElement;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.DomainResource;
import org.hl7.fhir.r4.model.ResourceType;
import org.hl7.fhir.r4.model.StringType;

/**
 * HAPI FHIR resource class for ViewDefinition from the SQL on FHIR specification.
 * <p>
 * This class allows HAPI to recognise and parse ViewDefinition resources. It mirrors the
 * structure of {@code au.csiro.pathling.views.FhirView} with HAPI annotations so that the JSON
 * structure is preserved during serialisation.
 * </p>
 *
 * @author John Grimes
 * @see <a
 * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition.html">ViewDefinition</a>
 */
@ResourceDef(name = "ViewDefinition",
    profile = "http://hl7.org/fhir/uv/sql-on-fhir/StructureDefinition/ViewDefinition")
public class ViewDefinitionResource extends DomainResource {

  @Child(name = "name", min = 0, max = 1)
  private StringType name;

  @Child(name = "resource", min = 1, max = 1)
  private CodeType resource;

  @Child(name = "status", min = 0, max = 1)
  private CodeType status;

  @Child(name = "select", min = 1, max = Child.MAX_UNLIMITED)
  private List<SelectComponent> select;

  @Child(name = "where", min = 0, max = Child.MAX_UNLIMITED)
  private List<WhereComponent> where;

  @Child(name = "constant", min = 0, max = Child.MAX_UNLIMITED)
  private List<ConstantComponent> constant;

  public StringType getNameElement() {
    return name;
  }

  public boolean hasNameElement() {
    return name != null && !name.isEmpty();
  }

  public void setNameElement(final StringType name) {
    this.name = name;
  }

  public StringType getName() {
    return name;
  }

  public void setName(final StringType name) {
    this.name = name;
  }

  public CodeType getResourceElement() {
    return resource;
  }

  public boolean hasResourceElement() {
    return resource != null && !resource.isEmpty();
  }

  public void setResourceElement(final CodeType resource) {
    this.resource = resource;
  }

  public CodeType getResource() {
    return resource;
  }

  public void setResource(final CodeType resource) {
    this.resource = resource;
  }

  public CodeType getStatusElement() {
    return status;
  }

  public boolean hasStatusElement() {
    return status != null && !status.isEmpty();
  }

  public void setStatusElement(final CodeType status) {
    this.status = status;
  }

  public CodeType getStatus() {
    return status;
  }

  public void setStatus(final CodeType status) {
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

  public void setSelect(final List<SelectComponent> select) {
    this.select = select;
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

  public void setWhere(final List<WhereComponent> where) {
    this.where = where;
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

  public void setConstant(final List<ConstantComponent> constant) {
    this.constant = constant;
  }

  @Override
  public DomainResource copy() {
    final ViewDefinitionResource copy = new ViewDefinitionResource();
    copyValues(copy);
    copy.name = name != null ? name.copy() : null;
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
        && (resource == null || resource.isEmpty())
        && (status == null || status.isEmpty())
        && (select == null || select.isEmpty())
        && (where == null || where.isEmpty())
        && (constant == null || constant.isEmpty());
  }

  /**
   * Select clause component.
   */
  @Block
  public static class SelectComponent extends BackboneElement {

    @Child(name = "column", min = 0, max = Child.MAX_UNLIMITED)
    private List<ColumnComponent> column;

    @Child(name = "select", min = 0, max = Child.MAX_UNLIMITED)
    private List<SelectComponent> select;

    @Child(name = "forEach", min = 0, max = 1)
    private StringType forEach;

    @Child(name = "forEachOrNull", min = 0, max = 1)
    private StringType forEachOrNull;

    @Child(name = "unionAll", min = 0, max = Child.MAX_UNLIMITED)
    private List<SelectComponent> unionAll;

    public List<ColumnComponent> getColumn() {
      if (column == null) {
        column = new ArrayList<>();
      }
      return column;
    }

    public boolean hasColumn() {
      return column != null && !column.isEmpty();
    }

    public void setColumn(final List<ColumnComponent> column) {
      this.column = column;
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

    public void setSelect(final List<SelectComponent> select) {
      this.select = select;
    }

    public StringType getForEachElement() {
      return forEach;
    }

    public boolean hasForEachElement() {
      return forEach != null && !forEach.isEmpty();
    }

    public void setForEachElement(final StringType forEach) {
      this.forEach = forEach;
    }

    public StringType getForEach() {
      return forEach;
    }

    public void setForEach(final StringType forEach) {
      this.forEach = forEach;
    }

    public StringType getForEachOrNullElement() {
      return forEachOrNull;
    }

    public boolean hasForEachOrNullElement() {
      return forEachOrNull != null && !forEachOrNull.isEmpty();
    }

    public void setForEachOrNullElement(final StringType forEachOrNull) {
      this.forEachOrNull = forEachOrNull;
    }

    public StringType getForEachOrNull() {
      return forEachOrNull;
    }

    public void setForEachOrNull(final StringType forEachOrNull) {
      this.forEachOrNull = forEachOrNull;
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

    public void setUnionAll(final List<SelectComponent> unionAll) {
      this.unionAll = unionAll;
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
      return copy;
    }

    @Override
    public boolean isEmpty() {
      return super.isEmpty() && (column == null || column.isEmpty())
          && (select == null || select.isEmpty())
          && forEach == null && forEachOrNull == null
          && (unionAll == null || unionAll.isEmpty());
    }
  }

  /**
   * Column component.
   */
  @Block
  public static class ColumnComponent extends BackboneElement {

    @Child(name = "name", min = 1, max = 1)
    private StringType name;

    @Child(name = "path", min = 1, max = 1)
    private StringType path;

    @Child(name = "description", min = 0, max = 1)
    private StringType description;

    @Child(name = "collection", min = 0, max = 1)
    private BooleanType collection;

    @Child(name = "type", min = 0, max = 1)
    private StringType type;

    @Child(name = "tag", min = 0, max = Child.MAX_UNLIMITED)
    private List<TagComponent> tag;

    public StringType getNameElement() {
      return name;
    }

    public boolean hasNameElement() {
      return name != null && !name.isEmpty();
    }

    public void setNameElement(final StringType name) {
      this.name = name;
    }

    public StringType getName() {
      return name;
    }

    public void setName(final StringType name) {
      this.name = name;
    }

    public StringType getPathElement() {
      return path;
    }

    public boolean hasPathElement() {
      return path != null && !path.isEmpty();
    }

    public void setPathElement(final StringType path) {
      this.path = path;
    }

    public StringType getPath() {
      return path;
    }

    public void setPath(final StringType path) {
      this.path = path;
    }

    public StringType getDescriptionElement() {
      return description;
    }

    public boolean hasDescriptionElement() {
      return description != null && !description.isEmpty();
    }

    public void setDescriptionElement(final StringType description) {
      this.description = description;
    }

    public StringType getDescription() {
      return description;
    }

    public void setDescription(final StringType description) {
      this.description = description;
    }

    public BooleanType getCollectionElement() {
      return collection;
    }

    public boolean hasCollectionElement() {
      return collection != null && !collection.isEmpty();
    }

    public void setCollectionElement(final BooleanType collection) {
      this.collection = collection;
    }

    public BooleanType getCollection() {
      return collection;
    }

    public void setCollection(final BooleanType collection) {
      this.collection = collection;
    }

    public StringType getTypeElement() {
      return type;
    }

    public boolean hasTypeElement() {
      return type != null && !type.isEmpty();
    }

    public void setTypeElement(final StringType type) {
      this.type = type;
    }

    public StringType getType() {
      return type;
    }

    public void setType(final StringType type) {
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

    public void setTag(final List<TagComponent> tag) {
      this.tag = tag;
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
      return super.isEmpty() && name == null && path == null
          && description == null && collection == null && type == null
          && (tag == null || tag.isEmpty());
    }
  }

  /**
   * Tag component for columns.
   */
  @Block
  public static class TagComponent extends BackboneElement {

    @Child(name = "name", min = 1, max = 1)
    private StringType name;

    @Child(name = "value", min = 0, max = 1)
    private StringType value;

    public StringType getNameElement() {
      return name;
    }

    public boolean hasNameElement() {
      return name != null && !name.isEmpty();
    }

    public void setNameElement(final StringType name) {
      this.name = name;
    }

    public StringType getName() {
      return name;
    }

    public void setName(final StringType name) {
      this.name = name;
    }

    public StringType getValueElement() {
      return value;
    }

    public boolean hasValueElement() {
      return value != null && !value.isEmpty();
    }

    public void setValueElement(final StringType value) {
      this.value = value;
    }

    public StringType getValue() {
      return value;
    }

    public void setValue(final StringType value) {
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

  /**
   * Where clause component.
   */
  @Block
  public static class WhereComponent extends BackboneElement {

    @Child(name = "path", min = 1, max = 1)
    private StringType path;

    @Child(name = "description", min = 0, max = 1)
    private StringType description;

    public StringType getPathElement() {
      return path;
    }

    public boolean hasPathElement() {
      return path != null && !path.isEmpty();
    }

    public void setPathElement(final StringType path) {
      this.path = path;
    }

    public StringType getPath() {
      return path;
    }

    public void setPath(final StringType path) {
      this.path = path;
    }

    public StringType getDescriptionElement() {
      return description;
    }

    public boolean hasDescriptionElement() {
      return description != null && !description.isEmpty();
    }

    public void setDescriptionElement(final StringType description) {
      this.description = description;
    }

    public StringType getDescription() {
      return description;
    }

    public void setDescription(final StringType description) {
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

  /**
   * Constant component.
   */
  @Block
  public static class ConstantComponent extends BackboneElement {

    @Child(name = "name", min = 1, max = 1)
    private StringType name;

    @Child(name = "value", min = 0, max = 1, type = {StringType.class, BooleanType.class})
    private org.hl7.fhir.r4.model.Type value;

    public StringType getNameElement() {
      return name;
    }

    public boolean hasNameElement() {
      return name != null && !name.isEmpty();
    }

    public void setNameElement(final StringType name) {
      this.name = name;
    }

    public StringType getName() {
      return name;
    }

    public void setName(final StringType name) {
      this.name = name;
    }

    public org.hl7.fhir.r4.model.Type getValue() {
      return value;
    }

    public boolean hasValue() {
      return value != null && !value.isEmpty();
    }

    public void setValue(final org.hl7.fhir.r4.model.Type value) {
      this.value = value;
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
