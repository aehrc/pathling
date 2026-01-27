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

package au.csiro.pathling.fhirpath.collection;

import static au.csiro.pathling.fhirpath.TypeSpecifier.FHIR_NAMESPACE;
import static au.csiro.pathling.fhirpath.TypeSpecifier.SYSTEM_NAMESPACE;
import static au.csiro.pathling.utilities.Preconditions.check;

import au.csiro.pathling.encoders.ExtensionSupport;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.TerminologyConcepts;
import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.collection.mixed.MixedCollection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.comparison.Equatable;
import au.csiro.pathling.fhirpath.definition.ChildDefinition;
import au.csiro.pathling.fhirpath.definition.ChoiceDefinition;
import au.csiro.pathling.fhirpath.definition.ElementDefinition;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import au.csiro.pathling.fhirpath.function.ColumnTransform;
import com.google.common.collect.ImmutableMap;
import jakarta.annotation.Nonnull;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a collection of nodes that are the result of evaluating a FHIRPath expression.
 *
 * @author John Grimes
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
@Slf4j
public class Collection implements Equatable {

  // Additional mappings for collection classes that don't directly map to FhirPathType
  @Nonnull
  private static final Map<FHIRDefinedType, Class<? extends Collection>>
      ADDITIONAL_COLLECTION_MAPPINGS =
          new ImmutableMap.Builder<FHIRDefinedType, Class<? extends Collection>>()
              .put(FHIRDefinedType.REFERENCE, ReferenceCollection.class)
              .put(FHIRDefinedType.NULL, EmptyCollection.class)
              .build();

  // See https://hl7.org/fhir/fhirpath.html#types.

  /** A {@link Column} representing the result of evaluating this expression. */
  @Nonnull private final ColumnRepresentation column;

  /** The type of the result of evaluating this expression, if known. */
  @Nonnull private final Optional<FhirPathType> type;

  /** The FHIR type of the result of evaluating this expression, if there is one. */
  @Nonnull private final Optional<FHIRDefinedType> fhirType;

  /** The FHIR definition that describes this path, if there is one. */
  @Nonnull private final Optional<? extends NodeDefinition> definition;

  @Nonnull private final Optional<Column> extensionMapColumn;

  /**
   * Builds the appropriate subtype of {@link Collection} based upon the supplied {@link
   * ElementDefinition}.
   *
   * <p>Use this builder when the path may need to be traversable.
   *
   * @param columnRepresentation a {@link Column} containing the result of the expression
   * @param fhirType the {@link FHIRDefinedType} that this path should be based upon
   * @param definition the {@link ElementDefinition} that this path should be based upon
   * @return a new {@link Collection}
   * @throws CollectionConstructionError if there is a problem constructing the collection
   */
  @Nonnull
  public static Collection build(
      @Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final FHIRDefinedType fhirType,
      @Nonnull final Optional<ElementDefinition> definition) {
    return getInstance(columnRepresentation, Optional.of(fhirType), definition, Optional.empty());
  }

  /**
   * Builds the appropriate subtype of {@link Collection} based upon the supplied {@link
   * ElementDefinition}.
   *
   * <p>Use this builder when the path is the child of another path, and will need to be
   * traversable.
   *
   * @param columnRepresentation a {@link Column} containing the result of the expression
   * @param extensionMapColumn an optional extension map column
   * @param definition the {@link ElementDefinition} that this path should be based upon
   * @return a new {@link Collection}
   * @throws CollectionConstructionError if there is a problem constructing the collection
   */
  @Nonnull
  public static Collection build(
      @Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<Column> extensionMapColumn,
      @Nonnull final ElementDefinition definition) {
    final Optional<FHIRDefinedType> optionalFhirType = definition.getFhirType();
    if (optionalFhirType.isPresent()) {
      return getInstance(
          columnRepresentation, optionalFhirType, Optional.of(definition), extensionMapColumn);
    } else {
      throw new IllegalArgumentException(
          "Attempted to build a Collection with an ElementDefinition with no fhirType");
    }
  }

  /**
   * Builds the appropriate subtype of {@link Collection} based upon the supplied {@link
   * FHIRDefinedType}.
   *
   * <p>Use this builder when the path is derived, e.g. the result of a function.
   *
   * @param columnRepresentation a {@link ColumnRepresentation} containing the result of the
   *     expression
   * @param fhirType the {@link FHIRDefinedType} that this path should be based upon
   * @return a new {@link Collection}
   * @throws CollectionConstructionError if there is a problem constructing the collection
   */
  @Nonnull
  public static Collection build(
      @Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final FHIRDefinedType fhirType) {
    return getInstance(
        columnRepresentation, Optional.of(fhirType), Optional.empty(), Optional.empty());
  }

  /**
   * Builds the appropriate subtype of {@link Collection} based upon the supplied {@link
   * ColumnRepresentation}, {@link FHIRDefinedType} and {@link ElementDefinition}.
   *
   * @param columnRepresentation a {@link ColumnRepresentation} containing the result of the
   *     expression
   * @param fhirType the {@link FHIRDefinedType} that this path should be based upon
   * @param definition the {@link ElementDefinition} that this path should be based upon
   * @param extensionMapColumn an optional {@link Column} representing the extension map, if this
   *     path is an extension
   * @return a new {@link Collection} representing the specified path
   * @throws CollectionConstructionError if there is a problem constructing the collection
   */
  @Nonnull
  private static Collection getInstance(
      @Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final Optional<ElementDefinition> definition,
      @Nonnull final Optional<Column> extensionMapColumn) {
    // Look up the class that represents an element with the specified FHIR type.
    final FHIRDefinedType resolvedType =
        fhirType
            .or(() -> definition.flatMap(ElementDefinition::getFhirType))
            .orElseThrow(
                () -> {
                  // Check if this is a choice element selection scenario.
                  if (definition.isPresent() && definition.get().isChoiceElement()) {
                    final String elementName = definition.get().getElementName();
                    return new IllegalArgumentException(
                        "Selection of mixed collection not supported: " + elementName);
                  }
                  return new IllegalArgumentException("Must have a fhirType or a definition");
                });
    final Class<? extends Collection> elementPathClass =
        classForType(resolvedType).orElse(Collection.class);
    final Optional<FhirPathType> fhirPathType = FhirPathType.forFhirType(resolvedType);

    try {
      // Call its constructor and return.
      final Constructor<? extends Collection> constructor =
          elementPathClass.getDeclaredConstructor(
              ColumnRepresentation.class,
              Optional.class,
              Optional.class,
              Optional.class,
              Optional.class);
      return constructor.newInstance(
          columnRepresentation, fhirPathType, fhirType, definition, extensionMapColumn);
    } catch (final NoSuchMethodException
        | InstantiationException
        | IllegalAccessException
        | InvocationTargetException e) {
      throw new CollectionConstructionError("Problem building a Collection object", e);
    }
  }

  /**
   * Gets the collection class for a given FHIR type.
   *
   * @param fhirType a {@link FHIRDefinedType}
   * @return The subtype of {@link Collection} that represents this type
   */
  @Nonnull
  public static Optional<Class<? extends Collection>> classForType(
      @Nonnull final FHIRDefinedType fhirType) {
    // First check if there's a direct mapping through FhirPathType
    final Optional<FhirPathType> pathType = FhirPathType.forFhirType(fhirType);
    if (pathType.isPresent()) {
      return Optional.of(pathType.get().getCollectionClass());
    }
    // If not, check additional mappings
    return Optional.ofNullable(ADDITIONAL_COLLECTION_MAPPINGS.get(fhirType));
  }

  /**
   * Returns a new {@link StringCollection} representing the String representation of the input
   * using the Spark SQL cast to string type.
   *
   * @param collection The input collection
   * @return A new {@link StringCollection} representing the String representation of the input
   */
  protected static StringCollection defaultAsStringPath(@Nonnull final Collection collection) {
    return collection.asSingular().map(ColumnRepresentation::asString, StringCollection::build);
  }

  /**
   * Return the child {@link Collection} that results from traversing to the given elementName.
   *
   * @param elementName the name of the child element
   * @return a new {@link Collection} representing the child element
   */
  @Nonnull
  public Optional<Collection> traverse(@Nonnull final String elementName) {
    // We use the implementation of getChildElement in the definition to get the child definition.
    final Optional<ChildDefinition> maybeChildDef =
        definition.flatMap(def -> def.getChildElement(elementName));

    // There are two paths here:
    // 1. If the child is an extension, we have special behaviour for traversing to the extension.
    // 2. If the child is a regular element, we use the standard traversal method.
    return maybeChildDef.flatMap(
        childDef -> {
          if (ExtensionSupport.EXTENSION_ELEMENT_NAME().equals(elementName)) {
            check(
                maybeChildDef.get() instanceof ElementDefinition,
                "Expected an ElementDefinition for an extension");
            return traverseExtension((ElementDefinition) childDef);
          }
          return Optional.of(traverseChild(childDef));
        });
  }

  /**
   * Return the child {@link Collection} that results from traversing to the given child definition.
   *
   * @param childDefinition the child definition
   * @return a new {@link Collection} representing the child element
   */
  @Nonnull
  protected Collection traverseChild(@Nonnull final ChildDefinition childDefinition) {
    // There are two paths here:
    // 1. If the child is a choice, we have special behaviour for traversing to the choice that
    //    results in a mixed collection.
    // 2. If the child is a regular element, we use the standard traversal method.
    switch (childDefinition) {
      case final ChoiceDefinition choiceChildDefinition -> {
        return MixedCollection.buildElement(this, choiceChildDefinition);
      }
      case final ElementDefinition elementChildDefinition -> {
        if (elementChildDefinition.isChoiceElement()) {
          log.warn(
              "Traversing a choice element `{}` without using ofType() is not portable and may not"
                  + " work in some FHIRPath implementations. Consider using ofType() to specify the"
                  + " type of element you want to traverse.",
              elementChildDefinition.getElementName());
        }
        return traverseElement(elementChildDefinition);
      }
      default ->
          throw new IllegalArgumentException(
              "Unsupported child definition type: " + childDefinition.getClass().getSimpleName());
    }
  }

  /**
   * Return the child {@link Collection} that results from traversing to the given child element
   * definition.
   *
   * @param childDef the child element definition
   * @return a new {@link Collection} representing the child element
   */
  @Nonnull
  public Collection traverseElement(@Nonnull final ElementDefinition childDef) {
    // Invoke the traversal method on the column context to get the new column.
    final ColumnRepresentation columnRepresentation =
        getColumn().traverse(childDef.getElementName(), childDef.getFhirType());
    // Return a new Collection with the new column and the child definition.
    return Collection.build(columnRepresentation, extensionMapColumn, childDef);
  }

  /**
   * Traverses to an extension element within this collection.
   *
   * @param extensionDefinition the definition of the extension to traverse to
   * @return an optional collection representing the extension
   */
  @Nonnull
  protected Optional<Collection> traverseExtension(
      @Nonnull final ElementDefinition extensionDefinition) {
    return getExtensionMapColumn()
        .map(
            em ->
                Collection.build(
                    new DefaultRepresentation(em)
                        .transform(c -> getFid().applyTo(c).removeNulls().getValue())
                        .removeNulls()
                        .flatten(),
                    extensionMapColumn,
                    extensionDefinition));
  }

  /**
   * Gets the field ID column for this collection.
   *
   * @return the column representation containing the field ID
   */
  @Nonnull
  protected ColumnRepresentation getFid() {
    return column.traverse(ExtensionSupport.FID_FIELD_NAME());
  }

  /**
   * Returns a new {@link Collection} with the specified {@link ColumnRepresentation}.
   *
   * @param newValue The new {@link ColumnRepresentation} to use
   * @return A new {@link Collection} with the specified {@link ColumnRepresentation}
   * @throws CollectionConstructionError if there was a problem constructing the collection
   */
  @Nonnull
  public Collection copyWith(@Nonnull final ColumnRepresentation newValue) {
    if (definition.isPresent()) {
      final NodeDefinition definitionValue = definition.get();
      check(
          definitionValue instanceof ElementDefinition,
          "Cannot copy a Collection with a non-ElementDefinition definition");
      final ElementDefinition elementDefinition = (ElementDefinition) definitionValue;
      return getInstance(
          newValue, getFhirType(), Optional.of(elementDefinition), extensionMapColumn);
    }
    return getInstance(newValue, getFhirType(), Optional.empty(), extensionMapColumn);
  }

  /**
   * Returns a new {@link Collection} with the specified {@link Column}, preserving type and
   * extension information.
   *
   * <p>This is a convenience method that wraps the provided column in a {@link
   * DefaultRepresentation} and creates a new collection while maintaining the FHIR type and
   * extension mapping from the original collection. This is particularly useful when transforming
   * column data while preserving the collection's semantic context.
   *
   * @param newColumn The new {@link Column} to use as the collection's data
   * @return A new {@link Collection} with the specified {@link Column} but preserving FHIR type,
   *     extension information, and column representation type
   * @throws CollectionConstructionError if there was a problem constructing the collection
   */
  @Nonnull
  public Collection copyWithColumn(@Nonnull final Column newColumn) {
    // Use the current column's copyOf method to preserve the representation type.
    // This is important for ResourceRepresentation to maintain flat schema behavior.
    return copyWith(getColumn().copyOf(newColumn));
  }

  /**
   * Filters the elements of this collection using the specified lambda.
   *
   * <p>The lambda is applied using the same representation type as this collection's column,
   * preserving flat schema behavior when filtering resource collections.
   *
   * @param lambda The lambda to use for filtering
   * @return A new collection representing the filtered elements
   */
  @Nonnull
  public Collection filter(@Nonnull final ColumnTransform lambda) {
    return map(ctx -> ctx.filter(col -> lambda.apply(ctx.copyOf(col)).getValue()));
  }

  /**
   * Checks if this collection is statically known to be empty.
   *
   * <p>This method performs a static type check to determine if the collection is an {@link
   * EmptyCollection}. It does not evaluate the actual data or count elements at runtime. A
   * collection that is not statically empty may still contain zero elements when evaluated.
   *
   * @return {@code true} if this collection is an {@link EmptyCollection}, {@code false} otherwise
   */
  public boolean isEmpty() {
    return this instanceof EmptyCollection;
  }

  /**
   * Checks if this collection is statically known to be non-empty.
   *
   * <p>This method performs a static type check to determine if the collection is not an {@link
   * EmptyCollection}. It does not evaluate the actual data or count elements at runtime. A
   * collection that is statically non-empty may still contain zero elements when evaluated.
   *
   * @return {@code true} if this collection is not an {@link EmptyCollection}, {@code false}
   *     otherwise
   */
  public boolean isNotEmpty() {
    return !isEmpty();
  }

  /**
   * Returns a new collection representing the elements of this collection as a singular value.
   *
   * @param errorMessage the error message to produce if the collection cannot be singularized.
   * @return A new collection representing the elements of this collection as a singular value
   */
  @Nonnull
  public Collection asSingular(@Nonnull final String errorMessage) {
    return map(cr -> cr.singular(errorMessage));
  }

  /**
   * Returns a new collection representing the elements of this collection as a singular value.
   *
   * @return A new collection representing the elements of this collection as a singular value
   */
  @Nonnull
  public Collection asSingular() {
    return map(ColumnRepresentation::singular);
  }

  /**
   * Returns a new collection representing the elements of this collection as a plural value.
   *
   * @return A new collection representing the elements of this collection as a plural value
   */
  @Nonnull
  public Collection asPlural() {
    return map(ColumnRepresentation::plural);
  }

  /**
   * Returns a new collection with new values determined by the specified lambda.
   *
   * @param mapper The lambda to use for mapping
   * @return A new collection with new values determined by the specified lambda
   */
  @Nonnull
  public Collection map(@Nonnull final ColumnTransform mapper) {
    return copyWith(mapper.apply(getColumn()));
  }

  /**
   * Returns a new collection with new values determined by the specified lambda.
   *
   * @param mapper The lambda to use for mapping
   * @param constructor The constructor to use for the new collection
   * @param <C> The type of collection to return
   * @return A new collection with new values determined by the specified lambda
   */
  @Nonnull
  public <C extends Collection> C map(
      @Nonnull final ColumnTransform mapper,
      @Nonnull final Function<ColumnRepresentation, C> constructor) {
    return constructor.apply(mapper.apply(getColumn()));
  }

  /**
   * Returns a new collection with the same type and representation, the colum value of which is
   * computed by the lambda based on the current column value.
   *
   * @param columnMapper The lambda to use for mapping
   * @return A new collection with new values determined by the specified lambda
   */
  @Nonnull
  public Collection mapColumn(@Nonnull final UnaryOperator<Column> columnMapper) {
    return map(cr -> cr.map(columnMapper));
  }

  /**
   * Returns the {@link Column} value of this collection.
   *
   * @return The {@link Column} value of this collection
   */
  @Nonnull
  public Column getColumnValue() {
    return column.getValue();
  }

  /**
   * Returns a new collection representing just the elements of this collection with the specified
   * type.
   *
   * @param type The type of element to return
   * @return A new collection representing just the elements of this collection with the specified
   *     type
   */
  @Nonnull
  public Collection filterByType(@Nonnull final TypeSpecifier type) {
    final Optional<Collection> maybeCollection =
        switch (type.getNamespace()) {
          case SYSTEM_NAMESPACE -> getType().filter(type.toSystemType()::equals).map(t -> this);
          case FHIR_NAMESPACE -> getFhirType().filter(type.toFhirType()::equals).map(t -> this);
          default -> Optional.empty();
        };
    return maybeCollection.orElse(EmptyCollection.getInstance());
  }

  /**
   * Checks if this collection's single item is of the specified type.
   *
   * <p>This method implements the FHIRPath {@code is()} function behavior:
   *
   * <ul>
   *   <li>Throws an error if the collection contains more than one item
   *   <li>Returns an empty collection if this collection is statically empty
   *   <li>Returns a BooleanCollection with:
   *       <ul>
   *         <li>{@code null} values where the input is empty at runtime
   *         <li>{@code true} where the value matches the specified type
   *         <li>{@code false} where the value does not match the specified type
   *       </ul>
   * </ul>
   *
   * <p>The implementation leverages {@link #asBooleanSingleton()} to enforce the singular
   * constraint and {@link #filterByType(TypeSpecifier)} to determine type matches. The result is
   * computed lazily using Spark SQL operations, allowing efficient evaluation across large
   * datasets.
   *
   * @param type the type specifier to check against
   * @return a collection with boolean values indicating type match, or empty based on emptiness
   * @throws au.csiro.pathling.errors.InvalidUserInputError if the collection contains more than one
   *     item
   * @see <a href="https://hl7.org/fhirpath/#istype--type-specifier">FHIRPath is() function</a>
   */
  @Nonnull
  public Collection isOfType(@Nonnull final TypeSpecifier type) {
    // Static check for EmptyCollection - return empty immediately
    if (this instanceof EmptyCollection) {
      return EmptyCollection.getInstance();
    } else {
      // Convert to boolean singleton (enforces singular constraint), then check if
      // filtering by type produces a non-null value
      return asBooleanSingleton()
          .mapColumn(c -> functions.when(c, filterByType(type).getColumnValue().isNotNull()));
    }
  }

  /**
   * Returns the value if this collection's single item is of the specified type.
   *
   * <p>This method implements the FHIRPath {@code as()} function behavior:
   *
   * <ul>
   *   <li>Throws an error if the collection contains more than one item
   *   <li>Returns an empty collection if this collection is statically empty
   *   <li>Returns:
   *       <ul>
   *         <li>The input value where it matches the specified type
   *         <li>Empty collection where the value does not match the specified type
   *         <li>Empty collection where the input is empty at runtime
   *       </ul>
   * </ul>
   *
   * <p>The implementation leverages {@link #asSingular()} to enforce the singular constraint and
   * {@link #filterByType(TypeSpecifier)} to determine type matches. The result is computed lazily
   * using Spark SQL operations, allowing efficient evaluation across large datasets.
   *
   * @param type the type specifier to check against
   * @return the value if it matches the type, or empty collection otherwise
   * @throws au.csiro.pathling.errors.InvalidUserInputError if the collection contains more than one
   *     item
   * @see <a href="https://hl7.org/fhirpath/#astype--type-specifier">FHIRPath as() function</a>
   */
  @Nonnull
  public Collection asType(@Nonnull final TypeSpecifier type) {
    // Static check for EmptyCollection - return empty immediately
    if (this instanceof EmptyCollection) {
      return EmptyCollection.getInstance();
    } else {
      // Enforce singular constraint first, then filter by type
      return asSingular().filterByType(type);
    }
  }

  /**
   * Gets a user-friendly representation of the current collection that can be used to refer to it
   * in user errors.
   *
   * @return a user-friendly representation of the collection.
   */
  @Nonnull
  public String getDisplayExpression() {
    final String leftDisplay = getType().map(FhirPathType::getTypeSpecifier).orElse("unknown");
    return getFhirType().map(fdt -> leftDisplay + "(" + fdt.toCode() + ")").orElse(leftDisplay);
  }

  /**
   * Returns an optional {@link TerminologyConcepts} representation of this collection.
   *
   * @return An optional {@link TerminologyConcepts} representation of this collection
   */
  @Nonnull
  public Optional<TerminologyConcepts> toConcepts() {
    return getFhirType()
        .filter(FHIRDefinedType.CODEABLECONCEPT::equals)
        .map(
            t ->
                TerminologyConcepts.union(
                    getColumn().getField("coding"),
                    (CodingCollection) traverse("coding").orElseThrow()));
  }

  /**
   * Checks if this collection can be converted to the other collection type.
   *
   * @param other the other collection
   * @return true if the other collection can be converted to the other collection type
   */
  public boolean convertibleTo(@Nonnull final Collection other) {
    return typeEquivalentWith(other);
  }

  /**
   * Check if this collection is type equivalent with the other collection. The equivalence is based
   * on the type and fhirType of the collections.
   *
   * @param other the other collection
   * @return true if the other collection is type equivalent with this collection
   */
  public boolean typeEquivalentWith(@Nonnull final Collection other) {
    // if one has a type then the other needs to have the same type
    if (type.isPresent() || other.type.isPresent()) {
      return type.equals(other.type);
    }
    if (fhirType.isPresent() || other.fhirType.isPresent()) {
      // otherwise if this can be either an empty literal or an element literal
      // in which case we need to check that the fhir types are the same
      return fhirType.equals(other.fhirType);
    } else {
      // this most likely is an empty collection or mixed collection
      return this == other;
    }
  }

  /**
   * Casts this collection to the type of another collection.
   *
   * <p>This method attempts to cast the current collection to match the type of the provided
   * collection. The cast will only succeed if the current collection is convertible to the target
   * collection type as determined by the {@link #convertibleTo(Collection)} method.
   *
   * @param other The collection whose type to cast to
   * @return A new collection with the same values but cast to the type of the other collection
   * @throws IllegalArgumentException If this collection cannot be cast to the type of the other
   *     collection
   */
  @Nonnull
  public Collection castAs(@Nonnull final Collection other) {
    if (typeEquivalentWith(other)) {
      return this;
    } else if (convertibleTo(other)) {
      return other
          .getType()
          .map(castType -> other.map(t -> this.getColumn().elementCast(castType.getSqlDataType())))
          .orElse(this);
    } else {
      throw new IllegalArgumentException("Cannot cast " + this + " to " + other);
    }
  }

  /**
   * Gets a new BooleanCollection representing the Boolean representation of this path.
   *
   * @return a new {@link Collection} representing the Boolean representation of this path
   */
  @Nonnull
  public BooleanCollection asBooleanPath() {
    throw new InvalidUserInputError("Cannot implicitly convert " + this + " to BooleanCollection");
  }

  /**
   * Gets a new {@link BooleanCollection} representing this collection as a singular Boolean value.
   * Throws an exception during evaluation if the collection is not singular.
   *
   * @return a new {@link Collection} represented as singular Boolean value.
   */
  @Nonnull
  public BooleanCollection asBooleanSingleton() {
    return asSingular().map(ColumnRepresentation::toBoolean, BooleanCollection::build);
  }

  /**
   * Creates a new collection from the given FHIR resource value.
   *
   * @param value the FHIR resource value to convert to a collection
   * @return a new collection representing the FHIR resource value
   * @throws CollectionConstructionError if there is a problem constructing the collection
   */
  @Nonnull
  public static Collection fromValue(@Nonnull final IBase value) {
    final FHIRDefinedType fhirType = FHIRDefinedType.fromCode(value.fhirType());

    // Get the collection class for the FHIR type.
    final Class<? extends Collection> collectionClass =
        Collection.classForType(fhirType)
            .orElseThrow(
                () -> new InvalidUserInputError("Unsupported constant type: " + fhirType.toCode()));
    try {
      // Invoke the fromValue method on the collection class to get the return value.
      final Object returnValue =
          collectionClass.getMethod("fromValue", value.getClass()).invoke(null, value);
      check(returnValue instanceof Collection);
      return (Collection) returnValue;
    } catch (final IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new CollectionConstructionError("Problem constructing collection from value", e);
    }
  }
}
