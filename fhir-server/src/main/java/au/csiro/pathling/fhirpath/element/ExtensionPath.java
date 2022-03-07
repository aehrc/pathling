/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.element;

import au.csiro.pathling.encoders2.ExtensionSupport;
import au.csiro.pathling.fhirpath.ResourcePath;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
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
      @Nonnull final Optional<ResourcePath> foreignResource,
      @Nonnull final Optional<Column> thisColumn,
      @Nonnull final Enumerations.FHIRDefinedType fhirType) {
    super(expression, dataset, idColumn, eidColumn, valueColumn, singular, foreignResource,
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
