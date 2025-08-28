/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2025 Commonwealth Scientific and Industrial Research
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
 *
 */

package au.csiro.pathling.encoders

import au.csiro.pathling.encoders.datatypes.DataTypeMappings
import ca.uhn.fhir.context.FhirContext


/**
 * Configuration setting for encoders.
 */
trait EncoderSettings {

  /**
   * Gets the max nesting that recursive data types should be expanded to.
   * Zero indicates that fields ot type T should not be expanded in the composite of type T.
   *
   * @return the max nesting that recursive data types should be expanded to
   */
  def maxNestingLevel: Int

  /**
   * The list of types that are encoded within open types, such as extensions.
   */
  def openTypes: Set[String]

  /**
   * Indicates if support for FHIR extension should be enabled.
   *
   * @return if support for FHIR extension should be enabled.
   */
  def supportsExtensions: Boolean

  /**
   * Indicates if unique field ids (_fid) should be added to the schema. This is required
   * to support extension but it's an option at the moment to allow for backward compatibility with V1 encodes.
   * Once V2 has matured this can be removed together with all V1 related classes and tests.
   *
   * @return if unique field ids (_fid) should be added to the schema.
   */
  def generateFid: Boolean
}

/**
 * Access to common objects required by schema processing operations.
 */
trait EncoderContext extends EncoderSettings {

  def fhirContext: FhirContext

  def dataTypeMappings: DataTypeMappings

  def config: EncoderSettings

  override def maxNestingLevel: Int = config.maxNestingLevel

  override def openTypes: Set[String] = config.openTypes

  override def supportsExtensions: Boolean = config.supportsExtensions

  override def generateFid: Boolean = config.generateFid
}

/**
 * Default implementation of [[EncoderSettings]].
 *
 * @param maxNestingLevel    @see [[EncoderSettings.maxNestingLevel]]
 * @param openTypes          @see [[EncoderSettings.openTypes]]
 * @param supportsExtensions @see [[EncoderSettings.supportsExtensions]]
 * @param generateFid        @see [[EncoderSettings.generateFid]]
 */
case class EncoderConfig(override val maxNestingLevel: Int,
                         override val openTypes: Set[String],
                         override val supportsExtensions: Boolean,
                         override val generateFid: Boolean) extends EncoderSettings {
  assert(generateFid || !supportsExtensions, "includeFid must be enabled to support extensions")
}

object EncoderConfig {
  def apply(maxNestingLevel: Int, openTypes: Set[String],
            supportsExtensions: Boolean): EncoderConfig = EncoderConfig(maxNestingLevel, openTypes,
    supportsExtensions, generateFid = true)

  def apply(maxNestingLevel: Int, openTypes: Set[String]): EncoderConfig = EncoderConfig(
    maxNestingLevel, openTypes, supportsExtensions = true, generateFid = true)
}
