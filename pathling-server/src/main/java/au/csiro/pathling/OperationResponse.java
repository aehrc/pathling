package au.csiro.pathling;

/**
 * @author Felix Naumann
 */
public interface OperationResponse<T> {

    T toOutput();
}
