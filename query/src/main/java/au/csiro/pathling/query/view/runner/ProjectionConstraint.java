package au.csiro.pathling.query.view.runner;

/**
 * Describes the type of result that is intended to be returned from the extract query. This
 * influences the way that the expressions are validated during parsing - some data types may not be
 * supported by the result type.
 *
 * @author John Grimes
 */
public enum ProjectionConstraint {
  /**
   * Indicates that the result will be returned in a form that supports the representation of
   * complex types.
   */
  UNCONSTRAINED,

  /**
   * Indicates that the result will be returned in a form that will be constrained to a flat,
   * unstructured representation.
   */
  FLAT
}
