package au.csiro.pathling.fhirpath.evaluation;

import au.csiro.pathling.fhirpath.collection.Collection;
import ca.uhn.fhir.context.FhirContext;
import org.apache.spark.sql.SparkSession;

public record DefaultEvaluationContext(SparkSession spark, FhirContext fhirContext,
                                       Collection inputContext) implements EvaluationContext {

}
