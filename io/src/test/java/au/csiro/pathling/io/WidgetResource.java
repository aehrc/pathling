/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.io;

import ca.uhn.fhir.model.api.annotation.Child;
import ca.uhn.fhir.model.api.annotation.ResourceDef;
import jakarta.annotation.Nullable;
import java.io.Serial;
import org.hl7.fhir.r4.model.DomainResource;
import org.hl7.fhir.r4.model.ResourceType;
import org.hl7.fhir.r4.model.StringType;

/**
 * A resource type that is not part of FHIR, which stands for the custom types that definitions may
 * describe, such as {@code ViewDefinition}, in tests of the routes that parse with HAPI.
 */
@ResourceDef(name = "Widget")
public class WidgetResource extends DomainResource {

  @Serial private static final long serialVersionUID = 1L;

  @Nullable
  @Child(name = "label")
  private StringType label;

  @Nullable
  public StringType getLabelElement() {
    return label;
  }

  public WidgetResource setLabel(@Nullable final String value) {
    this.label = value == null ? null : new StringType(value);
    return this;
  }

  @Override
  public DomainResource copy() {
    final WidgetResource copy = new WidgetResource();
    copyValues(copy);
    copy.label = label == null ? null : label.copy();
    return copy;
  }

  @Nullable
  @Override
  public ResourceType getResourceType() {
    // A custom resource type has no entry in the enumeration.
    return null;
  }

  @Override
  public String fhirType() {
    return "Widget";
  }

  @Override
  public boolean isEmpty() {
    return super.isEmpty() && (label == null || label.isEmpty());
  }
}
