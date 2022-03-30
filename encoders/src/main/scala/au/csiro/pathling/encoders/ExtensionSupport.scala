/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © 2018-2022, Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230. Licensed
 * under the CSIRO Open Source Software Licence Agreement.
 *
 */

package au.csiro.pathling.encoders

/**
 * Helper class with constants related to FHIR extension support.
 */
object ExtensionSupport {

  val FID_FIELD_NAME: String = "_fid"
  val EXTENSIONS_FIELD_NAME: String = "_extension"

  val EXTENSION_ELEMENT_NAME: String = "extension"
}
