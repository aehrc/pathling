/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.element;

import static au.csiro.pathling.QueryHelpers.createColumn;

import au.csiro.pathling.QueryHelpers.DatasetWithColumn;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents any FHIRPath expression which refers to an element within a resource.
 *
 * @author John Grimes
 */
public class ElementPath extends NonLiteralPath {

  /**
   * The FHIR data type of the element being represented by this expression.
   * <p>
   * Note that there can be multiple valid FHIR types for a given FHIRPath type, e.g. {@code uri}
   * and {@code code} both map to the {@code String} FHIRPath type.
   *
   * @see <a href="https://hl7.org/fhir/fhirpath.html#types">Using FHIR types in expressions</a>
   */
  @Getter
  @Nonnull
  private final FHIRDefinedType fhirType;

  @Getter(AccessLevel.PROTECTED)
  @Nonnull
  private Optional<ElementDefinition> definition = Optional.empty();

  protected ElementPath(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Column idColumn, @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final Optional<ResourcePath> foreignResource,
      @Nonnull final Optional<Column> thisColumn, @Nonnull final FHIRDefinedType fhirType) {
    super(expression, dataset, idColumn, valueColumn, singular, foreignResource, thisColumn);
    this.fhirType = fhirType;
  }

  /**
   * Builds the appropriate subtype of ElementPath based upon the supplied {@link
   * ElementDefinition}.
   * <p>
   * Use this builder when the path is the child of another path, and will need to be traversable.
   *
   * @param expression the FHIRPath representation of this path
   * @param dataset a {@link Dataset} that can be used to evaluate this path against data
   * @param idColumn a {@link Column} within the dataset containing the identity of the subject
   * resource
   * @param valueColumn a {@link Column} within the dataset containing the values of the nodes
   * @param singular an indicator of whether this path represents a single-valued collection
   * @param foreignResource a foreign resource this path originated from, if any
   * @param thisColumn collection values where this path originated from {@code $this}
   * @param definition the HAPI element definition that this path should be based upon
   * @return a new ElementPath
   */
  @Nonnull
  public static ElementPath build(@Nonnull final String expression,
      @Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn,
      @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final Optional<ResourcePath> foreignResource,
      @Nonnull final Optional<Column> thisColumn,
      @Nonnull final ElementDefinition definition) {
    final Optional<FHIRDefinedType> optionalFhirType = definition.getFhirType();
    if (optionalFhirType.isPresent()) {
      final FHIRDefinedType fhirType = optionalFhirType.get();
      final ElementPath path = ElementPath
          .build(expression, dataset, idColumn, valueColumn, singular, foreignResource, thisColumn,
              fhirType);
      path.definition = Optional.of(definition);
      return path;
    } else {
      throw new IllegalArgumentException(
          "Attempted to build an ElementPath with an ElementDefinition with no fhirType");
    }
  }

  /**
   * Builds the appropriate subtype of ElementPath based upon the supplied {@link FHIRDefinedType}.
   * <p>
   * Use this builder when the path is derived, e.g. the result of a function.
   *
   * @param expression the FHIRPath representation of this path
   * @param dataset a {@link Dataset} that can be used to evaluate this path against data
   * @param idColumn a {@link Column} within the dataset containing the identity of the subject
   * resource
   * @param valueColumn a {@link Column} within the dataset containing the values of the nodes
   * @param singular an indicator of whether this path represents a single-valued collection
   * @param foreignResource a foreign resource this path originated from, if any
   * @param thisColumn collection values where this path originated from {@code $this}
   * @param fhirType the FHIR type that this path should be based upon
   * @return a new ElementPath
   */
  @Nonnull
  public static ElementPath build(@Nonnull final String expression,
      @Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn,
      @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final Optional<ResourcePath> foreignResource,
      @Nonnull final Optional<Column> thisColumn, @Nonnull final FHIRDefinedType fhirType) {
    return getInstance(expression, dataset, idColumn, valueColumn, singular, foreignResource,
        thisColumn, fhirType);
  }

  @Nonnull
  private static ElementPath getInstance(@Nonnull final String expression,
      @Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn,
      @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final Optional<ResourcePath> foreignResource,
      @Nonnull final Optional<Column> thisColumn, @Nonnull final FHIRDefinedType fhirType) {
    // Look up the class that represents an element with the specified FHIR type.
    final Class<? extends ElementPath> elementPathClass = ElementDefinition
        .elementClassForType(fhirType).orElse(ElementPath.class);
    // Alias the value column.
    final DatasetWithColumn datasetWithColumn = createColumn(dataset, valueColumn);
    try {
      // Call its constructor and return.
      final Constructor<? extends ElementPath> constructor = elementPathClass
          .getDeclaredConstructor(String.class, Dataset.class, Column.class, Column.class,
              boolean.class, Optional.class, Optional.class, FHIRDefinedType.class);
      return constructor
          .newInstance(expression, datasetWithColumn.getDataset(), idColumn,
              datasetWithColumn.getColumn(), singular, foreignResource, thisColumn, fhirType);
    } catch (final NoSuchMethodException | InstantiationException | IllegalAccessException |
        InvocationTargetException e) {
      throw new RuntimeException("Problem building an ElementPath class", e);
    }
  }

  @Nonnull
  @Override
  public Optional<ElementDefinition> getChildElement(@Nonnull final String name) {
    return definition.flatMap(elementDefinition -> elementDefinition.getChildElement(name));
  }

  @Nonnull
  @Override
  public ElementPath copy(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Column idColumn, @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final Optional<Column> thisColumn) {
    return definition
        .map(elementDefinition -> ElementPath
            .build(expression, dataset, idColumn, valueColumn, singular, foreignResource,
                thisColumn, elementDefinition))
        .orElseGet(
            () -> ElementPath
                .build(expression, dataset, idColumn, valueColumn, singular, foreignResource,
                    thisColumn, fhirType));
  }

}
