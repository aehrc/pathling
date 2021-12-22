/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 *
 */

package au.csiro.pathling.encoders

/**
 * Thrown when encoding is requested for an unsupported resource type.
 * 
 * @param message a descriptive message to give back to the user
 */
class UnsupportedResourceError(message: String) extends RuntimeException(message) {}
