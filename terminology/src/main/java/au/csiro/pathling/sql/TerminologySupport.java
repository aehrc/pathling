package au.csiro.pathling.sql;

import au.csiro.pathling.sql.udf.TranslateUdf;
import au.csiro.pathling.utilities.Strings;
import org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.isNull;
import static java.util.function.Function.identity;

/**
 * Supporting functions for {@link Terminology} interface.
 */
public interface TerminologySupport {

  /**
   * Parses a string with the coma seprated list of concept map equivalence codes to the collection
   * of unique {@link ConceptMapEquivalence} enum values. Throws {@link
   * au.csiro.pathling.errors.InvalidUserInputError} if any of the codes cannot be converted.
   *
   * @param equivalencesCsv a collection of equivalence codes.
   * @return the set of unique enum values.
   */
  @Nullable
  static Set<ConceptMapEquivalence> parseCsvEquivalences(
      @Nullable final String equivalencesCsv) {
    return isNull(equivalencesCsv)
           ? null
           : equivalenceCodesToEnum(Strings.parseCsvList(equivalencesCsv, identity()));
  }

  /**
   * Converts a collection of concept map equivalence codes to the collection of unique {@link
   * ConceptMapEquivalence} enum values. Throws {@link au.csiro.pathling.errors.InvalidUserInputError}
   * if any of the codes cannot be converted.
   *
   * @param equivalenceCodes a collection of equivalence codes.
   * @return the set of unique enum values.
   */
  @Nullable
  static Set<ConceptMapEquivalence> equivalenceCodesToEnum(
      @Nullable final Collection<String> equivalenceCodes) {
    return isNull(equivalenceCodes)
           ? null
           : equivalenceCodes.stream()
               .map(TranslateUdf::checkValidEquivalenceCode)
               .map(ConceptMapEquivalence::fromCode)
               .collect(Collectors.toUnmodifiableSet());
  }
}
