/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.search;

import au.csiro.pathling.search.filter.DateMatcher;
import au.csiro.pathling.search.filter.ExactStringMatcher;
import au.csiro.pathling.search.filter.MatcherFactory;
import au.csiro.pathling.search.filter.NumberMatcher;
import au.csiro.pathling.search.filter.QuantityMatcher;
import au.csiro.pathling.search.filter.ReferenceMatcher;
import au.csiro.pathling.search.filter.SearchFilter;
import au.csiro.pathling.search.filter.StringMatcher;
import au.csiro.pathling.search.filter.TokenMatcher;
import au.csiro.pathling.search.filter.UriMatcher;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Set;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Enum representing the types of FHIR search parameters.
 *
 * <p>Each search parameter type has a set of allowed FHIR types that can be searched and implements
 * {@link MatcherFactory} to create appropriate search filters.
 *
 * <p>Implemented types override {@link #createFilter(String, FHIRDefinedType)} with type-specific
 * logic. Unimplemented types use the default implementation which throws {@link
 * UnsupportedOperationException}.
 *
 * @see <a href="https://hl7.org/fhir/search.html#ptypes">Search Parameter Types</a>
 */
public enum SearchParameterType implements MatcherFactory {

  /**
   * A token type search parameter matches a system and/or code. Supports the {@code :not} modifier
   * for negated matching.
   */
  TOKEN(
      Set.of(
          FHIRDefinedType.CODE,
          FHIRDefinedType.CODING,
          FHIRDefinedType.CODEABLECONCEPT,
          FHIRDefinedType.IDENTIFIER,
          FHIRDefinedType.CONTACTPOINT,
          FHIRDefinedType.BOOLEAN,
          FHIRDefinedType.STRING,
          FHIRDefinedType.URI,
          FHIRDefinedType.ID)) {
    @Nonnull
    @Override
    public SearchFilter createFilter(
        @Nullable final String modifier, @Nonnull final FHIRDefinedType fhirType) {
      if ("not".equals(modifier)) {
        return new SearchFilter(new TokenMatcher(fhirType), true);
      }
      if (modifier != null) {
        throw new InvalidModifierException(modifier, this);
      }
      return new SearchFilter(new TokenMatcher(fhirType));
    }
  },

  /**
   * A string type search parameter matches string values. Supports the {@code :exact} modifier for
   * case-sensitive exact matching. Default is case-insensitive prefix matching.
   */
  STRING(
      Set.of(
          FHIRDefinedType.STRING,
          FHIRDefinedType.HUMANNAME,
          FHIRDefinedType.ADDRESS,
          FHIRDefinedType.MARKDOWN)) {
    @Nonnull
    @Override
    public SearchFilter createFilter(
        @Nullable final String modifier, @Nonnull final FHIRDefinedType fhirType) {
      if ("exact".equals(modifier)) {
        return new SearchFilter(new ExactStringMatcher());
      }
      if (modifier != null) {
        throw new InvalidModifierException(modifier, this);
      }
      return new SearchFilter(new StringMatcher());
    }
  },

  /**
   * A date type search parameter matches date/time values. Handles both scalar date types (date,
   * dateTime, instant) and Period type.
   */
  DATE(
      Set.of(
          FHIRDefinedType.DATE,
          FHIRDefinedType.DATETIME,
          FHIRDefinedType.INSTANT,
          FHIRDefinedType.PERIOD)) {
    @Nonnull
    @Override
    public SearchFilter createFilter(
        @Nullable final String modifier, @Nonnull final FHIRDefinedType fhirType) {
      if (modifier != null) {
        throw new InvalidModifierException(modifier, this);
      }
      final DateMatcher matcher =
          fhirType == FHIRDefinedType.PERIOD
              ? DateMatcher.forPeriod()
              : DateMatcher.forScalarDates();
      return new SearchFilter(matcher);
    }
  },

  /**
   * A number type search parameter matches numeric values. For integer types (integer, positiveInt,
   * unsignedInt), exact match semantics are used. For decimal types, range-based semantics based on
   * significant figures are used.
   */
  NUMBER(
      Set.of(
          FHIRDefinedType.INTEGER, FHIRDefinedType.DECIMAL,
          FHIRDefinedType.POSITIVEINT, FHIRDefinedType.UNSIGNEDINT)) {
    @Nonnull
    @Override
    public SearchFilter createFilter(
        @Nullable final String modifier, @Nonnull final FHIRDefinedType fhirType) {
      if (modifier != null) {
        throw new InvalidModifierException(modifier, this);
      }
      return new SearchFilter(new NumberMatcher(fhirType));
    }
  },

  /**
   * A quantity type search parameter matches quantity values with optional units. Supports
   * value-only matching and will support UCUM unit normalization.
   */
  QUANTITY(Set.of(FHIRDefinedType.QUANTITY)) {
    @Nonnull
    @Override
    public SearchFilter createFilter(
        @Nullable final String modifier, @Nonnull final FHIRDefinedType fhirType) {
      if (modifier != null) {
        throw new InvalidModifierException(modifier, this);
      }
      return new SearchFilter(new QuantityMatcher());
    }
  },

  /**
   * A reference type search parameter matches references to other resources. Supports the {@code
   * :not} modifier for negated matching and the {@code :[type]} modifier to constrain the target
   * resource type of a polymorphic reference.
   */
  REFERENCE(Set.of(FHIRDefinedType.REFERENCE)) {
    @Nonnull
    @Override
    public SearchFilter createFilter(
        @Nullable final String modifier, @Nonnull final FHIRDefinedType fhirType) {
      if ("not".equals(modifier)) {
        return new SearchFilter(new ReferenceMatcher(), true);
      }
      if (modifier != null) {
        // Validate that the modifier is a known FHIR resource type name.
        try {
          ResourceType.fromCode(modifier);
        } catch (final FHIRException e) {
          throw new InvalidModifierException(modifier, this);
        }
        final ReferenceMatcher referenceMatcher = new ReferenceMatcher();
        final String type = modifier;
        return new SearchFilter(
            (element, searchValue) -> {
              // Prepend type to bare ID values; pass through already type-qualified values.
              final String qualifiedValue =
                  searchValue.contains("/") ? searchValue : type + "/" + searchValue;
              return referenceMatcher.match(element, qualifiedValue);
            });
      }
      return new SearchFilter(new ReferenceMatcher());
    }
  },

  /**
   * A URI type search parameter matches URI values using exact string equality. Supports the {@code
   * :not} modifier for negated matching, {@code :below} for prefix matching, and {@code :above} for
   * inverse prefix matching.
   */
  URI(
      Set.of(
          FHIRDefinedType.URI,
          FHIRDefinedType.URL,
          FHIRDefinedType.CANONICAL,
          FHIRDefinedType.OID,
          FHIRDefinedType.UUID)) {
    @Nonnull
    @Override
    public SearchFilter createFilter(
        @Nullable final String modifier, @Nonnull final FHIRDefinedType fhirType) {
      if ("not".equals(modifier)) {
        return new SearchFilter(UriMatcher.exact(), true);
      }
      if ("below".equals(modifier)) {
        return new SearchFilter(UriMatcher.below());
      }
      if ("above".equals(modifier)) {
        return new SearchFilter(UriMatcher.above());
      }
      if (modifier != null) {
        throw new InvalidModifierException(modifier, this);
      }
      return new SearchFilter(UriMatcher.exact());
    }
  },

  /** A composite type search parameter combines multiple parameters. Not yet implemented. */
  COMPOSITE(Set.of()),

  /** A special type search parameter has custom behavior. Not yet implemented. */
  SPECIAL(Set.of());

  @Nonnull private final Set<FHIRDefinedType> allowedFhirTypes;

  SearchParameterType(@Nonnull final Set<FHIRDefinedType> allowedFhirTypes) {
    this.allowedFhirTypes = allowedFhirTypes;
  }

  /**
   * Checks if the given FHIR type is allowed for this search parameter type.
   *
   * @param fhirType the FHIR type to check
   * @return true if the type is allowed, false otherwise
   */
  public boolean isAllowedFhirType(@Nonnull final FHIRDefinedType fhirType) {
    return allowedFhirTypes.contains(fhirType);
  }

  /**
   * Gets the set of FHIR types that are allowed for this search parameter type.
   *
   * @return the set of allowed FHIR types
   */
  @Nonnull
  public Set<FHIRDefinedType> getAllowedFhirTypes() {
    return allowedFhirTypes;
  }

  /**
   * Creates a search filter for this parameter type with the given modifier and FHIR type.
   *
   * <p>Implemented types override this method with type-specific logic. Unimplemented types use
   * this default implementation.
   *
   * @param modifier the search modifier (e.g., "not", "exact"), or null for no modifier
   * @param fhirType the FHIR type of the element being searched
   * @return a configured SearchFilter
   * @throws UnsupportedOperationException if this search parameter type is not yet implemented
   * @throws InvalidModifierException if the modifier is not supported for this type
   */
  @Nonnull
  @Override
  public SearchFilter createFilter(
      @Nullable final String modifier, @Nonnull final FHIRDefinedType fhirType) {
    throw new UnsupportedOperationException("Search parameter type not yet supported: " + this);
  }
}
