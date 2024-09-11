package au.csiro.pathling.fhirpath.expression;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.evaluation.EvaluationContext;
import au.csiro.pathling.fhirpath.operator.BinaryOperator;
import au.csiro.pathling.fhirpath.operator.BinaryOperatorInput;
import java.util.Optional;
import org.apache.commons.lang3.NotImplementedException;

public record BinaryOperatorCall(FhirPath left, FhirPath right, String operator) implements
    FhirPath {

  @Override
  public Collection evaluate(final Collection input, final EvaluationContext context) {
    final Optional<BinaryOperator> operator = context.operatorRegistry().getInstance(this.operator);
    if (operator.isEmpty()) {
      throw new NotImplementedException("Operator not implemented: " + this.operator);
    }
    final BinaryOperatorInput operatorInput = new BinaryOperatorInput(context, input, left, right);
    return operator.get().invoke(operatorInput);
  }

}
