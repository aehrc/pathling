package au.csiro.pathling;

/**
 * Represents the response of an operation.
 *
 * @author Felix Naumann
 */
public interface OperationResponse<T> {

  /**
   * Create an output object from this response.
   *
   * @return The output object.
   */
  T toOutput();
}
