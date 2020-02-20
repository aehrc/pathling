package au.csiro.pathling.test.fixtures;

import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;


/**
 * Represents an equivalence relation as defined in ConceptMap i.e: target -- equivalence -->
 * source. E.g. target -- subsumes --> source means target is more general.
 * 
 * @author szu004
 */
public class ConceptMapEntry {
  private final Coding source;
  private final Coding target;
  private final ConceptMapEquivalence equivalence;

  private ConceptMapEntry(Coding source, Coding target, ConceptMapEquivalence equivalence) {
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

  public static ConceptMapEntry of(Coding source, Coding target,
      ConceptMapEquivalence equivalence) {
    return new ConceptMapEntry(source, target, equivalence);
  }

  public static ConceptMapEntry ofSubsumes(Coding source, Coding target) {
    return of(source, target, ConceptMapEquivalence.SUBSUMES);
  }

  public static ConceptMapEntry subsumesOf(Coding source, Coding target) {
    return of(source, target, ConceptMapEquivalence.SUBSUMES);
  }

  public static ConceptMapEntry specializesOf(Coding source, Coding target) {
    return of(source, target, ConceptMapEquivalence.SPECIALIZES);
  }
}
