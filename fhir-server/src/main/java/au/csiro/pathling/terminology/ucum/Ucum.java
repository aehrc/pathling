/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.terminology.ucum;

import java.io.InputStream;
import javax.annotation.Nonnull;
import org.fhir.ucum.UcumEssenceService;
import org.fhir.ucum.UcumException;
import org.fhir.ucum.UcumService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Makes UCUM services available to the rest of the application.
 *
 * @author John Grimes
 */
@Component
@Profile({"core", "fhir"})
public class Ucum {

  public static final String SYSTEM_URI = "http://unitsofmeasure.org/";

  @Bean
  @Nonnull
  public static UcumService ucumEssenceService() throws UcumException {
    final InputStream essenceStream = Ucum.class.getClassLoader()
        .getResourceAsStream("tx/ucum-essence.xml");
    return new UcumEssenceService(essenceStream);
  }

}
