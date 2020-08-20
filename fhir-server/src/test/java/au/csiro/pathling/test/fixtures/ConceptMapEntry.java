/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.fixtures;

import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;


/**
 * Represents an equivalence relation as defined in ConceptMap i.e: target -- equivalence -->
 * source. E.g. target -- subsumes --> source means target is more general.
 *
 * @author Piotr Szul
 */
public class ConceptMapEntry {

  private final Coding source;
  private final Coding target;
  private final ConceptMapEquivalence equivalence;

  private ConceptMapEntry(final Coding source, final Coding target,
      final ConceptMapEquivalence equivalence) {
    this.source = source;
    this.target = target;
    this.equivalence = equivalence;
  }

  public Coding getSource() {
    return source;
  }

  public Coding getTarget() {
    return target;
  }

  public ConceptMapEquivalence getEquivalence() {
    return equivalence;
  }

  public static ConceptMapEntry of(final Coding source, final Coding target,
      final ConceptMapEquivalence equivalence) {
    return new ConceptMapEntry(source, target, equivalence);
  }

  public static ConceptMapEntry subsumesOf(final Coding source, final Coding target) {
    return of(source, target, ConceptMapEquivalence.SUBSUMES);
  }

  public static ConceptMapEntry specializesOf(final Coding source, final Coding target) {
    return of(source, target, ConceptMapEquivalence.SPECIALIZES);
  }
}
