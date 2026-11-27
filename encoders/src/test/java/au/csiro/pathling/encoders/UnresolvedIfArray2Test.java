/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2026 Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
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
package au.csiro.pathling.encoders;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;
import scala.Function1;

/**
 * Tests the failure of {@link UnresolvedIfArray2} over a value that is not an array, which is a
 * defect in the caller, and so must be reported in terms that identify it.
 *
 * @author Piotr Szul
 */
class UnresolvedIfArray2Test {

  @Test
  void nonArrayValueFailsWithReadableMessage() {
    final Function1<Expression, Expression> identity = x -> x;
    final UnresolvedIfArray2 ifArray2 =
        new UnresolvedIfArray2(new Literal(null, DataTypes.StringType), identity, identity);

    final AnalysisException error =
        assertThrows(AnalysisException.class, () -> ifArray2.mapChildren(identity));

    final String message = error.getMessage();
    assertTrue(message.contains("array"), message);
    assertTrue(message.contains("StringType"), message);
    assertFalse(message.contains("Cannot find main error class"), message);
  }
}
