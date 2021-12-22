/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © 2018-2021, Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230. Licensed
 * under the CSIRO Open Source Software Licence Agreement.
 */

package au.csiro.pathling.encoders;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.Patient;
import org.junit.Assert;
import org.junit.Test;

public class EncodingContextTest {

  private final FhirContext fhirContext = FhirContext.forR4();

  @Test
  public void testCorrectNestingLevels() {

    final RuntimeResourceDefinition patientDefinition = fhirContext
        .getResourceDefinition(Patient.class);

    final RuntimeResourceDefinition conditionDefinition = fhirContext
        .getResourceDefinition(Condition.class);

    // start with a new context
    EncodingContext.runWithContext(() -> {

      Assert.assertEquals(0, EncodingContext.currentNestingLevel(patientDefinition));
      Assert.assertEquals(0, EncodingContext.currentNestingLevel(conditionDefinition));

      // Enter Patient
      EncodingContext.withDefinition(patientDefinition, () -> {
        Assert.assertEquals(1, EncodingContext.currentNestingLevel(patientDefinition));
        Assert.assertEquals(0, EncodingContext.currentNestingLevel(conditionDefinition));

        // Enter Patient
        EncodingContext.withDefinition(patientDefinition, () -> {
          Assert.assertEquals(2, EncodingContext.currentNestingLevel(patientDefinition));
          Assert.assertEquals(0, EncodingContext.currentNestingLevel(conditionDefinition));

          // Enter Condition
          EncodingContext.withDefinition(conditionDefinition, () -> {
            Assert.assertEquals(2, EncodingContext.currentNestingLevel(patientDefinition));
            Assert.assertEquals(1, EncodingContext.currentNestingLevel(conditionDefinition));
            return null;
          });
          Assert.assertEquals(2, EncodingContext.currentNestingLevel(patientDefinition));
          Assert.assertEquals(0, EncodingContext.currentNestingLevel(conditionDefinition));
          return null;
        });
        Assert.assertEquals(1, EncodingContext.currentNestingLevel(patientDefinition));
        Assert.assertEquals(0, EncodingContext.currentNestingLevel(conditionDefinition));
        return null;
      });
      Assert.assertEquals(0, EncodingContext.currentNestingLevel(patientDefinition));
      Assert.assertEquals(0, EncodingContext.currentNestingLevel(conditionDefinition));
      return null;
    });
  }


  @Test
  public void testFailsWithoutContext() {

    final RuntimeResourceDefinition patientDefinition = fhirContext
        .getResourceDefinition(Patient.class);

    Assert.assertThrows("Current EncodingContext does not exists.", AssertionError.class,
        () -> EncodingContext.currentNestingLevel(patientDefinition));
  }


  @Test
  public void testFailsWhenNestedContextIsCreated() {

    EncodingContext.runWithContext(() -> {
      Assert.assertThrows("There should be no current context", AssertionError.class,
          () -> EncodingContext.runWithContext(() -> null));
      return null;
    });
  }

}
