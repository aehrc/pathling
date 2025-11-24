/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.projection;

import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.fhirpath.path.Paths.Traversal;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ProjectionClauseTest {

  @Test
  void testToExpressionTree() {

    // create a complex ProjectionClause implementation for testing

    final GroupingSelection selection = new GroupingSelection(
        List.of(
            new UnnestingSelection(new Traversal("path1"), new GroupingSelection(List.of()), false),
            new UnnestingSelection(new Traversal("path2"), new GroupingSelection(List.of()), true),
            new RepeatSelection(List.of(new Traversal("path3"), new Traversal("path4")),
                new GroupingSelection(List.of()), 10),
            new UnionSelection(
                List.of(new GroupingSelection(List.of()), new GroupingSelection(List.of()))),
            new ColumnSelection(
                List.of(
                    new RequestedColumn(new Traversal("col1"), "name1", false, Optional.empty(),
                        Optional.empty()),
                    new RequestedColumn(new Traversal("col2"), "name2", true, Optional.empty(),
                        Optional.empty())
                )
            )
        )
    );
    assertEquals("""
            group
              forEach: path1
                group
              forEachOrNull: path2
                group
              repeat: [path3, path4]
                group
              union
                group
                group
              columns[one: col1 as name1, many: col2 as name2]""",
        selection.toExpressionTree());
  }
}
