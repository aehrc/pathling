/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.operators;

import static au.csiro.clinsight.utilities.Strings.md5Short;
import static org.apache.spark.sql.functions.explode_outer;

import au.csiro.clinsight.fhir.definitions.PathResolver;
import au.csiro.clinsight.fhir.definitions.PathTraversal;
import au.csiro.clinsight.fhir.definitions.ResourceDefinitions;
import au.csiro.clinsight.fhir.definitions.exceptions.ElementNotKnownException;
import au.csiro.clinsight.fhir.definitions.exceptions.ResourceNotKnownException;
import au.csiro.clinsight.query.parsing.ParsedExpression;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirType;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Provides the ability to move from one element to its child element, using the path selection
 * notation ".".
 *
 * @author John Grimes
 * @see <a href="http://hl7.org/fhirpath/2018Sep/index.html#path-selection">http://hl7.org/fhirpath/2018Sep/index.html#path-selection</a>
 */
public class PathTraversalOperator {

  @Nonnull
  public ParsedExpression invoke(@Nonnull PathTraversalInput input) {
    validateInput(input);
    ParsedExpression left = input.getLeft();
    String right = input.getRight();
    Dataset<Row> prevDataset = left.getDataset();
    String prevColumn = left.getDatasetColumn();
    String prevIdColumn = left.getDatasetColumn() + "_id";

    // Resolve the path of the expression.
    PathTraversal pathTraversal;
    assert left.getPathTraversal()
        != null : "Encountered input to path traversal operator with no path traversal information";
    try {
      String path = left.getPathTraversal().getPath() + "." + right;
      pathTraversal = PathResolver.resolvePath(path);
    } catch (ResourceNotKnownException | ElementNotKnownException e) {
      throw new InvalidRequestException(e.getMessage());
    }
    String fhirTypeCode = pathTraversal.getElementDefinition().getTypeCode();
    FhirType fhirType = FhirType.forFhirTypeCode(fhirTypeCode);
    FhirPathType fhirPathType = FhirPathType.forFhirTypeCode(fhirTypeCode);
    boolean isPrimitive = ResourceDefinitions.isPrimitive(fhirTypeCode);
    boolean isSingular = pathTraversal.getElementDefinition().getMaxCardinality().equals("1");

    // Create a new dataset that contains the ID column and the new value (or the value exploded, if
    // the element has a max cardinality greater than one).
    String hash = md5Short(input.getExpression());
    Column field = prevDataset.col(prevColumn).getField(right);
    Column column = isSingular ? field : explode_outer(field);
    column = column.alias(hash);
    Column idColumn = prevDataset.col(prevIdColumn).alias(hash + "_id");
    Dataset<Row> dataset = prevDataset.select(idColumn, column);

    // Construct a new parse result.
    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(input.getExpression());
    result.setFhirPathType(fhirPathType);
    result.setFhirType(fhirType);
    result.setPrimitive(isPrimitive);
    result.setSingular(left.isSingular() && isSingular);
    result.setOrigin(left.getOrigin());
    result.setDataset(dataset);
    result.setDatasetColumn(hash);
    result.setPathTraversal(pathTraversal);

    return result;
  }

  private void validateInput(PathTraversalInput input) {
    ParsedExpression left = input.getLeft();
    if (left.isPrimitive()) {
      throw new InvalidRequestException(
          "Attempt to perform path traversal on primitive element: " + left.getFhirPath());
    }
  }

}
