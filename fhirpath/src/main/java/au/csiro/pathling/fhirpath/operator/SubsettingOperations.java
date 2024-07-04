package au.csiro.pathling.fhirpath.operator;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import javax.annotation.Nonnull;
import org.apache.spark.sql.functions;

public class SubsettingOperations {

  /**
   * Returns a singleton collection containing the element at the specified index.
   *
   * @param subject The collection to index
   * @param index The index to use
   * @return A singleton collection containing the element at the specified index
   * @see <a href="https://hl7.org/fhirpath/#index-integer-collection">Indexer operation</a>
   */
  @FhirPathOperator
  @Nonnull
  public static Collection index(@Nonnull final Collection subject,
      @Nonnull final IntegerCollection index) {
    return subject.map(
        rep -> rep.vectorize(
            // If the subject is non-singular, use the `element_at` function to extract the element 
            // at the specified index. 
            c -> functions.element_at(c, index.getColumnValue().plus(1)),
            // If the subject is singular, return the subject itself. 
            c -> c
        ));
  }
}
