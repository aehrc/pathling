/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.vcl;

/**
 * The filter operators of the VCL v1 grammar, each mapping to a {@code
 * ValueSet.compose.include.filter} operation.
 *
 * @author John Grimes
 */
public enum VclFilterOperator {
  /** {@code =} equality against a code. */
  EQUALS,
  /** {@code <<} is-a (self or descendant). */
  IS_A,
  /** {@code ~<<} is-not-a. */
  IS_NOT_A,
  /** {@code <} descendent-of (strict). */
  DESCENDENT_OF,
  /** {@code <!} child-of (direct children). */
  CHILD_OF,
  /** {@code !!<} descendent-leaf. */
  DESCENDENT_LEAF,
  /** {@code >>} generalizes (self or ancestor). */
  GENERALIZES,
  /** {@code /} regular expression match against a quoted string. */
  REGEX,
  /** {@code ^} in a value set, code list, or nested filter list. */
  IN,
  /** {@code ~^} not in a value set, code list, or nested filter list. */
  NOT_IN,
  /** {@code ?} existence of a property. */
  EXISTS
}
