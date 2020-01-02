/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.operators;

import static org.apache.spark.sql.functions.explode_outer;

import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementCompositeDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Provides the ability to move from one element to its child element, using the path selection
 * notation ".".
 *
 * @author John Grimes
 * @see <a href="https://pathling.app/docs/fhirpath/path-selection.html">Path selection</a>
 */
public class PathTraversalOperator {

  @Nonnull
  public ParsedExpression invoke(@Nonnull PathTraversalInput input) {
    validateInput(input);
    ParsedExpression left = input.getLeft();
    String right = input.getRight();
    Dataset<Row> leftDataset = left.getDataset();
    Column leftIdColumn = left.getIdColumn(),
        leftValueColumn = left.getValueColumn();

    // Determine type and cardinality from the definitions.
    BaseRuntimeChildDefinition childDefinition = null;
    if (left.isResource()) {
      RuntimeResourceDefinition resourceDefinition = input.getContext().getFhirContext()
          .getResourceDefinition(left.getResourceType().toCode());
      childDefinition = resourceDefinition.getChildByName(right);
    } else {
      BaseRuntimeElementDefinition elementDefinition = left.getElementDefinition();
      if (elementDefinition instanceof BaseRuntimeElementCompositeDefinition) {
        childDefinition = ((BaseRuntimeElementCompositeDefinition) elementDefinition)
            .getChildByName(right);
      } else {
        assert false : "Path traversal invoked on non-composite element";
      }
    }

    // Throw an error if the requested child was not found.
    if (childDefinition == null) {
      throw new InvalidRequestException("Unknown child of " + left.getFhirPath() + ": " + right);
    }

    // Get the FHIR and FHIRPath types from the child definition.
    FHIRDefinedType fhirType = ParsedExpression.fhirTypeFromDefinition(childDefinition, right);
    FhirPathType fhirPathType = FhirPathType.forFhirTypeCode(fhirType);
    boolean isSingular = childDefinition.getMax() == 1;
    boolean isPrimitive = fhirPathType != null && fhirPathType.isPrimitive();

    // Create a new dataset that contains the ID column and the new value (or the value exploded, if
    // the element has a max cardinality greater than one).
    Column field = leftValueColumn.getField(right);
    Column valueColumn = isSingular
        ? field
        : explode_outer(field);
    Dataset<Row> dataset;
    if (isSingular) {
      dataset = leftDataset;
    } else {
      // If we are exploding a field, we need to explicitly make a new column out of it. Row
      // generators can not be nested inside expressions.
      dataset = leftDataset.withColumn("explodeResult", valueColumn);
      valueColumn = dataset.col("explodeResult");
    }

    // Construct a new parse result.
    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(input.getExpression());
    result.setFhirPathType(fhirPathType);
    result.setFhirType(fhirType);
    result.setDefinition(childDefinition, right);
    result.setPrimitive(isPrimitive);
    result.setSingular(left.isSingular() && isSingular);
    result.setOrigin(left.getOrigin());
    result.setDataset(dataset);
    result.setHashedValue(leftIdColumn, valueColumn);

    return result;
  }

  private void validateInput(PathTraversalInput input) {
    ParsedExpression left = input.getLeft();
    if (left.isPrimitive()) {
      throw new InvalidRequestException(
          "Attempt to perform path traversal on primitive element: " + left.getFhirPath());
    }
    if (left.isPolymorphic()) {
      throw new InvalidRequestException(
          "Attempt at path traversal on polymorphic input: " + left.getFhirPath());
    }
  }

}
