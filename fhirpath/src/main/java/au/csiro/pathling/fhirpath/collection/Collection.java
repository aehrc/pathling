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

package au.csiro.pathling.fhirpath.collection;

import au.csiro.pathling.encoders.ExtensionSupport;
import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.Numeric;
import au.csiro.pathling.fhirpath.PathEvalContext;
import au.csiro.pathling.fhirpath.Reference;
import au.csiro.pathling.fhirpath.annotations.NotImplemented;
import au.csiro.pathling.fhirpath.column.ColumnCtx;
import au.csiro.pathling.fhirpath.column.StdColumnCtx;
import au.csiro.pathling.fhirpath.definition.ChildDefinition;
import au.csiro.pathling.fhirpath.definition.ChoiceChildDefinition;
import au.csiro.pathling.fhirpath.definition.ElementChildDefinition;
import au.csiro.pathling.fhirpath.definition.ElementDefinition;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import au.csiro.pathling.fhirpath.definition.ReferenceDefinition;
import au.csiro.pathling.utilities.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Represents a collection of nodes that are the result of evaluating a FHIRPath expression.
 *
 * @author John Grimes
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
public class Collection implements Comparable, Numeric {

  // See https://hl7.org/fhir/fhirpath.html#types.
  @Nonnull
  private static final Map<FHIRDefinedType, Class<? extends Collection>> FHIR_TYPE_TO_ELEMENT_PATH_CLASS =
      new ImmutableMap.Builder<FHIRDefinedType, Class<? extends Collection>>()
          .put(FHIRDefinedType.BOOLEAN, BooleanCollection.class)
          .put(FHIRDefinedType.STRING, StringCollection.class)
          .put(FHIRDefinedType.URI, StringCollection.class)
          .put(FHIRDefinedType.URL, StringCollection.class)
          .put(FHIRDefinedType.CANONICAL, StringCollection.class)
          .put(FHIRDefinedType.CODE, StringCollection.class)
          .put(FHIRDefinedType.OID, StringCollection.class)
          .put(FHIRDefinedType.ID, StringCollection.class)
          .put(FHIRDefinedType.UUID, StringCollection.class)
          .put(FHIRDefinedType.MARKDOWN, StringCollection.class)
          .put(FHIRDefinedType.BASE64BINARY, StringCollection.class)
          .put(FHIRDefinedType.INTEGER, IntegerCollection.class)
          .put(FHIRDefinedType.UNSIGNEDINT, IntegerCollection.class)
          .put(FHIRDefinedType.POSITIVEINT, IntegerCollection.class)
          .put(FHIRDefinedType.DECIMAL, DecimalCollection.class)
          .put(FHIRDefinedType.DATE, DateCollection.class)
          .put(FHIRDefinedType.DATETIME, DateTimeCollection.class)
          .put(FHIRDefinedType.INSTANT, DateTimeCollection.class)
          .put(FHIRDefinedType.TIME, TimeCollection.class)
          .put(FHIRDefinedType.CODING, CodingCollection.class)
          .put(FHIRDefinedType.QUANTITY, QuantityCollection.class)
          .put(FHIRDefinedType.SIMPLEQUANTITY, QuantityCollection.class)
          .build();
  /**
   * A {@link Column} representing the result of evaluating this expression.
   */
  @Nonnull
  private final ColumnCtx columnCtx;

  /**
   * The type of the result of evaluating this expression, if known.
   */
  @Nonnull
  private final Optional<FhirPathType> type;

  /**
   * The FHIR type of the result of evaluating this expression, if there is one.
   */
  @Nonnull
  private final Optional<FHIRDefinedType> fhirType;

  /**
   * The FHIR definition that describes this path, if there is one.
   */
  @Nonnull
  private final Optional<? extends NodeDefinition> definition;

  /**
   * Builds a generic {@link Collection} with the specified column, FHIRPath type, FHIR type and
   * definition.
   *
   * @param columnCtx a {@link Column} containing the result of the expression
   * @param fhirPathType the {@link FhirPathType} that this path should be based upon
   * @param fhirType the {@link FHIRDefinedType} that this path should be based upon
   * @param definition the {@link ElementDefinition} that this path should be based upon
   * @return a new {@link Collection}
   */
  @Nonnull
  public static Collection build(@Nonnull final ColumnCtx columnCtx,
      @Nonnull final Optional<FhirPathType> fhirPathType,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final Optional<? extends NodeDefinition> definition) {
    return new Collection(columnCtx, fhirPathType, fhirType, definition);
  }

  @Nonnull
  public static Collection build(@Nonnull final ColumnCtx columnCtx,
      @Nonnull final FHIRDefinedType fhirType,
      @Nonnull final Optional<ElementDefinition> definition) {
    return getInstance(columnCtx, Optional.of(fhirType), definition);
  }

  /**
   * Builds the appropriate subtype of {@link Collection} based upon the supplied
   * {@link ElementDefinition}.
   * <p>
   * Use this builder when the path is the child of another path, and will need to be traversable.
   *
   * @param columnCtx a {@link Column} containing the result of the expression
   * @param definition the {@link ElementDefinition} that this path should be based upon
   * @return a new {@link Collection}
   */
  @Nonnull
  public static Collection build(@Nonnull final ColumnCtx columnCtx,
      @Nonnull final ElementDefinition definition) {
    final Optional<FHIRDefinedType> optionalFhirType = definition.getFhirType();
    if (optionalFhirType.isPresent()) {
      return getInstance(columnCtx, optionalFhirType, Optional.of(definition));
    } else {
      throw new IllegalArgumentException(
          "Attempted to build a Collection with an ElementDefinition with no fhirType");
    }
  }

  /**
   * Builds the appropriate subtype of {@link Collection} based upon the supplied
   * {@link FHIRDefinedType}.
   * <p>
   * Use this builder when the path is derived, e.g. the result of a function.
   *
   * @param columnCtx a {@link ColumnCtx} containing the result of the expression
   * @param fhirType the {@link FHIRDefinedType} that this path should be based upon
   * @return a new {@link Collection}
   */
  @Nonnull
  public static Collection build(@Nonnull final ColumnCtx columnCtx,
      @Nonnull final FHIRDefinedType fhirType) {
    return getInstance(columnCtx, Optional.of(fhirType), Optional.empty());
  }

  @Nonnull
  private static Collection getInstance(@Nonnull final ColumnCtx columnCtx,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final Optional<ElementDefinition> definition) {
    // Look up the class that represents an element with the specified FHIR type.
    final FHIRDefinedType resolvedType = fhirType
        .or(() -> definition.flatMap(ElementDefinition::getFhirType))
        .orElseThrow(() -> new IllegalArgumentException("Must have a fhirType or a definition"));
    final Class<? extends Collection> elementPathClass = classForType(resolvedType).orElse(
        Collection.class);
    final Optional<FhirPathType> fhirPathType = FhirPathType.forFhirType(resolvedType);

    try {
      // Call its constructor and return.
      final Constructor<? extends Collection> constructor = elementPathClass
          .getDeclaredConstructor(ColumnCtx.class, Optional.class, Optional.class, Optional.class);
      return constructor
          .newInstance(columnCtx, fhirPathType, fhirType, definition);
    } catch (final NoSuchMethodException | InstantiationException | IllegalAccessException |
                   InvocationTargetException e) {
      throw new RuntimeException("Problem building a Collection object", e);
    }
  }

  /**
   * @param fhirType A {@link FHIRDefinedType}
   * @return The subtype of {@link Collection} that represents this type
   */
  @Nonnull
  public static Optional<Class<? extends Collection>> classForType(
      @Nonnull final FHIRDefinedType fhirType) {
    return Optional.ofNullable(FHIR_TYPE_TO_ELEMENT_PATH_CLASS.get(fhirType));
  }

  /**
   * Return the child {@link Collection} that results from traversing to the given elementName.
   *
   * @param elementName the name of the child element
   * @return a new {@link Collection} representing the child element
   */
  @Nonnull
  public Optional<Collection> traverse(@Nonnull final String elementName) {

    final Optional<? extends ChildDefinition> maybeChildDef = definition.flatMap(
            def -> def.getChildElement(elementName))
        .filter(ChildDefinition.class::isInstance);

    return maybeChildDef.flatMap(
        childDef -> ExtensionSupport.EXTENSION_ELEMENT_NAME().equals(elementName)
                    ? traverseExtensions(childDef)
                    : Optional.of(traverseChild(childDef)));
  }

  @Nonnull
  protected Collection traverseChild(@Nonnull final ChildDefinition childDef) {
    // It is only possible to traverse to a child with an element definition.
    if (childDef instanceof ChoiceChildDefinition) {
      return MixedCollection.buildElement(this, (ChoiceChildDefinition) childDef);
    } else if (childDef instanceof ElementChildDefinition) {
      return traverseElement((ElementChildDefinition) childDef);
    } else {
      throw new IllegalArgumentException("Unsupported child definition type: " + childDef
          .getClass().getSimpleName());
    }
  }


  @Nonnull
  protected Collection traverseElement(@Nonnull final ElementDefinition childDef) {
    // It is only possible to traverse to a child with an element definition.
    return Collection.build(getCtx().traverse(childDef.getElementName()), childDef);
  }

  @Nonnull
  @NotImplemented
  protected Optional<Collection> traverseExtensions(
      @Nonnull final ChildDefinition extensionDefinition) {
    // check the provided definition is of an extension
    Preconditions.checkArgument(extensionDefinition instanceof ElementDefinition,
        "Cannot traverse to an extension with a non-ElementDefinition");
    return getExtensionMap().map(em ->
        Collection.build(
            // We need here to deal with the situation where _fid is an array of element ids
            getFid().transform(em::apply).flatten(),
            (ElementDefinition) extensionDefinition)
    );
  }

  @Nonnull
  protected ColumnCtx getFid() {
    return columnCtx.traverse(ExtensionSupport.FID_FIELD_NAME());
  }

  @Nonnull
  protected Optional<Column> getExtensionMap() {
    // TODO: This most likely needs to be implemented a
    // as a member column propagated from the parent resource/collection
    return Optional.of(functions.col(ExtensionSupport.EXTENSIONS_FIELD_NAME()));
  }

  /**
   * @return whether the order of the collection returned by this expression has any meaning
   */
  public boolean isOrderable() {
    return true;
  }

  @Nonnull
  @Override
  public Function<Comparable, Column> getComparison(@Nonnull final ComparisonOperation operation) {
    return Comparable.buildComparison(this, operation);
  }

  @Override
  public boolean isComparableTo(@Nonnull final Collection path) {
    return path.getClass().equals(this.getClass()) || path.getClass().equals(Collection.class);
  }

  @Nonnull
  @Override
  public Function<Numeric, Collection> getMathOperation(@Nonnull final MathOperation operation) {
    return input -> this;
  }

  @Nonnull
  @Override
  public Optional<Column> getNumericValueColumn() {
    return Optional.empty();
  }

  @Nonnull
  @Override
  public Optional<Column> getNumericContextColumn() {
    return Optional.empty();
  }


  /**
   * Creates a null {@link Collection}.
   *
   * @return the null collection.
   */
  @Nonnull
  public static Collection nullCollection() {
    return new Collection(ColumnCtx.nullCtx(), Optional.empty(), Optional.of(FHIRDefinedType.NULL),
        Optional.empty());
  }

  @Nonnull
  public Collection copyWith(@Nonnull final ColumnCtx newValue) {
    return getInstance(newValue, getFhirType(), (Optional<ElementDefinition>) definition);
  }

  @Nonnull
  public Collection filter(@Nonnull final Function<ColumnCtx, ColumnCtx> lambda) {
    return map(ctx -> ctx.filter(col -> lambda.apply(StdColumnCtx.of(col)).getValue()));
  }

  @Nonnull
  public Collection asSingular() {
    return map(ColumnCtx::singular);
  }

  @Nonnull
  public Collection map(@Nonnull final Function<ColumnCtx, ColumnCtx> mapper) {
    return copyWith(mapper.apply(getCtx()));
  }

  @Nonnull
  public <C extends Collection> C map(@Nonnull final Function<ColumnCtx, ColumnCtx> mapper,
      @Nonnull final Function<ColumnCtx, C> constructor) {
    return constructor.apply(mapper.apply(getCtx()));
  }


  @Nonnull
  public <C extends Collection> C flatMap(@Nonnull final Function<ColumnCtx, C> mapper) {
    return mapper.apply(getCtx());
  }

  @Nonnull
  public ColumnCtx getCtx() {
    return this.columnCtx;
  }


  @Nonnull
  public Optional<Reference> asReference(@Nonnull final PathEvalContext context) {
    return getFhirType()
        .filter(FHIRDefinedType.REFERENCE::equals)
        .map(__ -> new ReferenceImpl(this, context));
  }

  @Nonnull
  public Optional<CodingCollection> asCoding() {
    return getFhirType()
        .filter(FHIRDefinedType.CODEABLECONCEPT::equals)
        .map(__ -> (CodingCollection) traverse("coding").get());
  }

  @Nonnull
  public Column getColumn() {
    return columnCtx.getValue();
  }

  @Value
  private static class ReferenceImpl implements Reference {

    @Nonnull
    Collection referenceCollection;

    @Nonnull
    PathEvalContext context;

    @Nonnull
    @Override
    public ReferenceDefinition getDefinition() {
      return (ReferenceDefinition) referenceCollection.getDefinition()
          .get();
    }

    @Nonnull
    @Override
    public ResourceCollection reverseResolve(@Nonnull final ResourceCollection masterCollection,
        @Nonnull final ResourceType foreignResourceType) {

      // final ReferenceDefinition referenceDefinition = getDefinition();
      //
      // checkUserInput(
      //     referenceDefinition.getReferenceTypes().contains(masterCollection.getResourceType()),
      //     "Reference in argument to reverseResolve does not support input resource type: "
      //         + masterCollection.getResourceType());
      // // TODO: Finish
      // final ColumnCtx referenceCtx = referenceCollection.traverse("reference")
      //     .map(Collection::asSingular)
      //     .map(Collection::getCtx)
      //     .orElseThrow();
      //
      // return ResourceCollection.build(
      //     referenceCtx.reverseResolve(masterCollection.getResourceType(), foreignResourceType),
      //     context.getFhirContext(),
      //     foreignResourceType);
      throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public ResourceCollection resolve(@Nonnull final ResourceType foreignResourceType) {

      // final ColumnCtx referenceCtx = referenceCollection.traverse(
      //         "reference")
      //     .map(Collection::getCtx)
      //     .orElseThrow();
      //
      // return ResourceCollection.build(
      //     referenceCtx.resolve(foreignResourceType),
      //     context.getFhirContext(),
      //     foreignResourceType);
      throw new UnsupportedOperationException();
    }
  }

  @Deprecated
  @Nonnull
  public String getExpression() {
    return "??";
  }

  @Deprecated
  public boolean isSingular() {
    return true;
  }


}