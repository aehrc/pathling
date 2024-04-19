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

import au.csiro.pathling.encoders.ExtensionSupport;
import au.csiro.pathling.fhirpath.ResourcePath;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Enumerations;

/**
 * Represents a FHIRPath expression that is an extension container.
 *
 * @author John Grimes
 * @see <a href="https://hl7.org/fhir/R4/extensibility.html">Extensibility</a>
 */
public class ExtensionPath extends ElementPath {

  protected ExtensionPath(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Column idColumn, @Nonnull final Optional<Column> eidColumn,
      @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final Optional<ResourcePath> currentResource,
      @Nonnull final Optional<Column> thisColumn,
      @Nonnull final Enumerations.FHIRDefinedType fhirType) {
    super(expression, dataset, idColumn, eidColumn, valueColumn, singular, currentResource,
        thisColumn, fhirType);
  }

  @Nonnull
  @Override
  public Optional<ElementDefinition> getChildElement(@Nonnull final String name) {
    // This code introspects the type of the extension container column to determine the valid child 
    // element names.
    final MapType mapType = (MapType) dataset.select(getExtensionContainerColumn()).schema()
        .fields()[0].dataType();
    final ArrayType arrayType = (ArrayType) mapType.valueType();
    final StructType structType = (StructType) arrayType.elementType();
    final List<String> fieldNames = Arrays.stream(structType.fields()).map(StructField::name)
        .collect(Collectors.toList());
    // Add the extension field name, so that we can support traversal to nested extensions.
    fieldNames.add(ExtensionSupport.EXTENSION_ELEMENT_NAME());

    // If the field is part of the encoded extension container, pass control to the generic code to 
    // determine the correct element definition.
    return fieldNames.contains(name)
           ? super.getChildElement(name)
           : Optional.empty();
  }

}
