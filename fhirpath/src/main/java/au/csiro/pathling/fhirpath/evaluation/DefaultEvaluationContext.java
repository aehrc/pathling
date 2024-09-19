package au.csiro.pathling.fhirpath.evaluation;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.function.registry.FunctionRegistry;
import au.csiro.pathling.fhirpath.operator.BinaryOperator;
import org.apache.spark.sql.SparkSession;

public record DefaultEvaluationContext(SparkSession spark,
                                       FunctionRegistry<NamedFunction<? extends Collection>> functionRegistry,
                                       FunctionRegistry<BinaryOperator> operatorRegistry) implements
    EvaluationContext {

}
