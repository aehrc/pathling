/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query;

import ca.uhn.fhir.model.api.annotation.Description;
import ca.uhn.fhir.model.primitive.InstantDt;
import ca.uhn.fhir.rest.annotation.Count;
import ca.uhn.fhir.rest.annotation.OptionalParam;
import ca.uhn.fhir.rest.annotation.Search;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.param.StringParam;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.instance.model.api.IPrimitiveType;
import org.hl7.fhir.r4.model.Patient;

/**
 * @author John Grimes
 */
public class SelectProvider {

  public static final String SELECT_PARAMETER_NAME = "select";

  @Search(type = Patient.class)
  public IBundleProvider search(
      @OptionalParam(name = SELECT_PARAMETER_NAME)
      @Description(shortDefinition = "Filter resources based on a FHIRPath expression")
          StringParam select, @Count Integer count) {
    InstantDt searchTime = InstantDt.withCurrentTime();
    return new IBundleProvider() {
      @Override
      public IPrimitiveType<Date> getPublished() {
        return searchTime;
      }

      @Nonnull
      @Override
      public List<IBaseResource> getResources(int theFromIndex, int theToIndex) {
        return Collections.emptyList();
      }

      @Nullable
      @Override
      public String getUuid() {
        return null;
      }

      @Override
      public Integer preferredPageSize() {
        return count;
      }

      @Nullable
      @Override
      public Integer size() {
        return 0;
      }
    };
  }

}
