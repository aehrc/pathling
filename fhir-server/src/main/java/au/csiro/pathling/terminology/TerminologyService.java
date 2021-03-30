/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.terminology;

import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import java.util.Collection;
import java.util.Set;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;

/**
 * @author Piotr Szul
 * <p>
 * Abstraction layer for the terminology related operations.
 */
public interface TerminologyService {

  /**
   * Creates a translator for given set of codings according to the specified concept map. See also:
   * https://www.hl7.org/fhir/operation-conceptmap-translate.html.
   * <p>
   * Should be able to ignore codings including are undefined (i.e. the system or code is null).
   *
   * @param codings the collections of codings to find translations for.
   * @param conceptMapUrl the url of the concept map to use for translation.
   * @param reverse reverse true if true.
   * @param equivalences the equivalences to consider for translation.
   * @return the translator instance with requested translation.
   */
  @Nonnull
  ConceptTranslator translate(@Nonnull final Collection<SimpleCoding> codings,
      @Nonnull final String conceptMapUrl,
      boolean reverse, @Nonnull final Collection<ConceptMapEquivalence> equivalences);

  /**
   * Creates a transitive closure representation of subsumes relation for the given set of codings.
   * <p>
   * Should be able to ignore codings including are undefined (i.e. the system or code is null) or
   * where the system is unknown to the underlying terminology service.
   * <p>
   * Additional resources on closure table maintenance:
   * <a href="https://www.hl7.org/fhir/terminology-service.html#closure">Maintaining
   * a Closure Table</a>
   *
   * @param systemAndCodes the codings to construct the closure for.
   * @return the closure representation.
   */
  @Nonnull
  Relation getSubsumesRelation(@Nonnull final Collection<SimpleCoding> systemAndCodes);


  /**
   * Intersects the given set of codings with the <code>ValueSet</code> defined by provided uri.
   * <p>
   * Should be able to ignore codings including are undefined (i.e. the system or code is null) or
   * where the system is unknown to the underlying terminology service.
   *
   * @param valueSetUri the URI of the <code>ValueSet</code>.
   * @param systemAndCodes the collections of codings to intersect.
   * @return the set of input codings the belong to the <code>ValueSet</code>.
   */
  @Nonnull
  Set<SimpleCoding> intersect(@Nonnull final String valueSetUri,
      @Nonnull final Collection<SimpleCoding> systemAndCodes);

}