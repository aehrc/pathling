/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.test.assertions;

import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.annotations.NotImplemented;
import au.csiro.pathling.fhirpath.collection.Collection;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author John Grimes
 */
@SuppressWarnings("UnusedReturnValue")
@NotImplemented
public class LiteralPathAssertion extends BaseFhirPathAssertion<LiteralPathAssertion> {

  LiteralPathAssertion(@Nonnull final Collection result,
      @Nonnull final EvaluationContext evaluationContext) {
    super(result, evaluationContext);
  }

  //TODO: LiteralPathAssertion
  
  @Nonnull
  public LiteralPathAssertion has(@Nullable final Object expected) {
    // TODO: consider this implementation - we may want to evaluate the expression instead
    final Expression maybeLiteral = result.getColumn().expr();
    assertTrue(maybeLiteral instanceof Literal);
    assertEquals(expected, ((Literal) maybeLiteral).value());
    return this;
  }
  //
  // @Nonnull
  // public LiteralPathAssertion hasCodingValue(@Nonnull final Coding expectedCoding) {
  //   assertTrue(fhirPath instanceof CodingLiteralPath);
  //   final Coding actualCoding = ((CodingLiteralPath) fhirPath).getValue();
  //   assertTrue(expectedCoding.equalsDeep(actualCoding));
  //   return this;
  // }
}
