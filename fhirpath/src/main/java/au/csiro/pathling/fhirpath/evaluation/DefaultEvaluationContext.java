package au.csiro.pathling.fhirpath.evaluation;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.function.registry.FunctionRegistry;
import ca.uhn.fhir.context.FhirContext;
import org.apache.spark.sql.SparkSession;

public record DefaultEvaluationContext(SparkSession spark, FhirContext fhirContext,
                                       FunctionRegistry<NamedFunction<? extends Collection>> functionRegistry) implements
    EvaluationContext {

}
