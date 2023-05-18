package au.csiro.pathling.fhirpath.parser;

public enum UnnestBehaviour {
  /**
   * When a repeating element is encountered, it will be unnested.
   */
  UNNEST,

  /**
   * When a repeating element is encountered, an error will be thrown.
   */
  ERROR,

  /**
   * When a repeating element is encountered, it will be left as an array.
   */
  NOOP
}
