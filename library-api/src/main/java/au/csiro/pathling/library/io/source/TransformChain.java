package au.csiro.pathling.library.io.source;

import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * @author Felix Naumann
 */
public interface TransformChain<T, U> {
  
  public TransformChain<T,?> filter(Predicate<T> predicate);
  
  public TransformChain<?,U> map(UnaryOperator<U> unaryOperator);
}
