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

package au.csiro.pathling.encoders1;

import au.csiro.pathling.encoders.AbstractSchemaConverterTest;
import au.csiro.pathling.encoders.SchemaConverter;

public class SchemaConverter1Test extends AbstractSchemaConverterTest {

  @Override
  protected SchemaConverter createSchemaConverter(int maxNestingLevel) {
    return new SchemaConverter1(FHIR_CONTEXT, DATA_TYPE_MAPPINGS, maxNestingLevel);
  }
}
