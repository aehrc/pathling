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

package au.csiro.pathling.io;

/**
 * How often each of FR-016's exceptions applied in one round trip.
 *
 * <p>A count is returned for each rather than the exceptions being applied silently, so that a test
 * states how often it expects each to apply. An exception that never applied is then
 * distinguishable from one that was never needed, which matters when one is removed.
 */
public final class RoundTripOutcome {

  private final int primitiveMetadata;

  private final int inlineResources;

  private final int base64Whitespace;

  private final int numericOnly;

  private final int narrativeWhitespace;

  RoundTripOutcome(
      final int primitiveMetadata,
      final int inlineResources,
      final int base64Whitespace,
      final int numericOnly,
      final int narrativeWhitespace) {
    this.primitiveMetadata = primitiveMetadata;
    this.inlineResources = inlineResources;
    this.base64Whitespace = base64Whitespace;
    this.numericOnly = numericOnly;
    this.narrativeWhitespace = narrativeWhitespace;
  }

  /**
   * Returns the number of narratives that came back equal to the source only once their runs of
   * whitespace were collapsed, which HAPI's XML writer does to a narrative when a test writes its
   * XML input (decision 83).
   *
   * @return the count
   */
  public int getNarrativeWhitespace() {
    return narrativeWhitespace;
  }

  /**
   * Returns the number of primitive id and extension keys excluded until ticket 17.
   *
   * @return the count
   */
  public int getPrimitiveMetadata() {
    return primitiveMetadata;
  }

  /**
   * Returns the number of inline resources excluded because the element holding them is declared as
   * the abstract {@code Resource}, which the layout does not store.
   *
   * @return the count
   */
  public int getInlineResources() {
    return inlineResources;
  }

  /**
   * Returns the number of base64Binary values whose whitespace was removed before comparison,
   * because decoding and encoding again returns the same bytes on one line.
   *
   * @return the count
   */
  public int getBase64Whitespace() {
    return base64Whitespace;
  }

  /**
   * Returns the number of numbers that came back numerically equal to the source but with different
   * digits or scale, because a decimal is read and written through a double. A number that differs
   * only in the case of its exponent marker is not counted, because parsing does not keep it.
   *
   * @return the count
   */
  public int getNumericOnly() {
    return numericOnly;
  }
}
