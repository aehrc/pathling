package au.csiro.pathling.fhirpath;

import au.csiro.pathling.encoders.ValueFunctions;
import au.csiro.pathling.fhirpath.collection.CodingCollection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import jakarta.annotation.Nonnull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;

/**
 * Represents a set of concepts, which can be either a single concept or a union of multiple
 * concepts.
 */
public sealed interface Concepts permits Concepts.Set, Concepts.Union {

  /**
   * Apply a user defined function to this set of concepts.
   *
   * @param name the name of the UDF
   * @param args the arguments to the UDF
   * @return the result of applying the UDF
   */
  @Nonnull
  ColumnRepresentation apply(@Nonnull final String name,
      @Nonnull final ColumnRepresentation... args);


  /**
   * Flatten this set of concepts. This will convert any union of concepts into a set of concepts.
   *
   * @return a new set of concepts
   */
  @Nonnull
  Set flatten();

  @Nonnull
  CodingCollection getCodingTemplate();

  /**
   * Represents a collection of codings.  which should be treated as independent concepts.
   */
  @Value
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  class Set implements Concepts {

    ColumnRepresentation codings;
    CodingCollection codingTemplate;

    @Override
    @Nonnull
    public ColumnRepresentation apply(@Nonnull final String name,
        @Nonnull final ColumnRepresentation... args) {
      // here we are getting either a single coding or an array of codings
      // representing individual concepts 
      // so this is easy to discern - just transform with udf
      return codings.transformWithUdf(name, args);
    }

    @Override
    @Nonnull
    public Set flatten() {
      return this;
    }

  }

  /**
   * Represents a collection of union of codings which represent a single concept. E.g. from FHIR
   * CodeableConcept.
   */
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  @Value
  class Union implements Concepts {

    ColumnRepresentation codingUnion;
    CodingCollection codingTemplate;

    @Override
    @Nonnull
    public ColumnRepresentation apply(@Nonnull final String name,
        @Nonnull final ColumnRepresentation... args) {

      // here we are getting either a single array of codings
      // representing a single concept and an array of arrays of codings representing
      // multiple concepts each with a union of codings

      // hmm how can we decide what to use?
      // we would have to have something like is array of arrays? 
      // looks that we need another operator for this case

      return codingUnion.map(
          c -> ValueFunctions.ifArray2(c,
              // array of arrays
              aa -> new DefaultRepresentation(aa).transformWithUdf(name, args).getValue(),
              as -> new DefaultRepresentation(as).callUdf(name, args).getValue()
          ));
    }

    @Override
    @Nonnull
    public Set flatten() {
      return new Set(codingUnion.flatten(), codingTemplate);
    }
  }

  /**
   * Create as new set of concepts where each coding is treated as an independent concept.
   *
   * @param codingSet the column representation of the set of concepts
   * @param codingTemplate the {@link CodingCollection} to use as a template for coding results
   * @return a new set of concepts
   */
  @Nonnull
  static Concepts set(@Nonnull final ColumnRepresentation codingSet,
      @Nonnull final CodingCollection codingTemplate) {
    return new Set(codingSet, codingTemplate);
  }

  /**
   * Create a new union of concepts.
   *
   * @param codingUnion the column representation of the union of concepts
   * @param codingTemplate the {@link CodingCollection} to use as a template for coding results
   * @return a new union of concepts
   */
  @Nonnull
  static Concepts union(@Nonnull final ColumnRepresentation codingUnion,
      @Nonnull final CodingCollection codingTemplate) {
    return new Union(codingUnion, codingTemplate);
  }
}
