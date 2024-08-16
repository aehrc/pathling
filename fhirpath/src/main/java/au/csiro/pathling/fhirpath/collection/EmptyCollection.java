package au.csiro.pathling.fhirpath.collection;

import java.util.Optional;
import org.apache.spark.sql.functions;

public class EmptyCollection extends Collection {

  public EmptyCollection() {
    super(functions.array(), Optional.empty());
  }
  
}
