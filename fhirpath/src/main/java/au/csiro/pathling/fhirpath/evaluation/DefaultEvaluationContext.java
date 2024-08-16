package au.csiro.pathling.fhirpath.evaluation;

import ca.uhn.fhir.context.FhirContext;
import org.apache.spark.sql.SparkSession;

public record DefaultEvaluationContext(SparkSession spark, FhirContext fhirContext) implements
    EvaluationContext {

}
