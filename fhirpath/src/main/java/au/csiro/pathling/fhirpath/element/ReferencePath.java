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

package au.csiro.pathling.fhirpath.element;

import static au.csiro.pathling.utilities.Preconditions.check;

import au.csiro.pathling.fhirpath.Referrer;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.definition.ElementDefinition;
import au.csiro.pathling.fhirpath.definition.ReferenceDefinition;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Represents a FHIRPath expression which is a resource reference.
 *
 * @author John Grimes
 */
public class ReferencePath extends ElementPath implements Referrer {

  protected ReferencePath(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Column idColumn, @Nonnull final Column valueColumn,
      @Nonnull final Optional<Column> orderingColumn, final boolean singular,
      @Nonnull final Optional<ResourcePath> currentResource,
      @Nonnull final Optional<Column> thisColumn, @Nonnull final FHIRDefinedType fhirType) {
    super(expression, dataset, idColumn, valueColumn, orderingColumn, singular, currentResource,
        thisColumn, fhirType);
  }

  @Nonnull
  @Override
  public Optional<ReferenceDefinition> getDefinition() {
    final Optional<? extends ElementDefinition> definition = super.getDefinition();
    check(definition.isEmpty() || definition.get() instanceof ReferenceDefinition);
    //noinspection unchecked
    return (Optional<ReferenceDefinition>) definition;
  }

  @Nonnull
  public Set<ResourceType> getResourceTypes() {
    if (getDefinition().isPresent()) {
      return getDefinition().get().getReferenceTypes();
    } else {
      return Collections.emptySet();
    }
  }

  @Nonnull
  public Column getReferenceColumn() {
    return Referrer.referenceColumnFor(this);
  }

  @Nonnull
  @Override
  public Column getReferenceIdColumn() {
    return Referrer.referenceIdColumnFor(Referrer.referenceColumnFor(this));
  }

  @Nonnull
  public Column getResourceEquality(@Nonnull final ResourcePath resourcePath) {
    return Referrer.resourceEqualityFor(this, resourcePath);
  }

  @Nonnull
  public Column getResourceEquality(@Nonnull final Column targetId,
      @Nonnull final Column targetCode) {
    return Referrer.resourceEqualityFor(this, targetCode, targetId);
  }

  @Nonnull
  @Override
  public Optional<ElementDefinition> getChildElement(@Nonnull final String name) {
    // We only encode the reference and display elements of the Reference type.
    if (name.equals("reference") || name.equals("display")) {
      return super.getChildElement(name);
    } else {
      return Optional.empty();
    }
  }

}
