/*
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight.fhir;

import au.csiro.clinsight.persistence.Dimension;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.Read;
import ca.uhn.fhir.rest.annotation.Search;
import ca.uhn.fhir.rest.server.IResourceProvider;
import java.util.List;
import org.hibernate.Session;
import org.hl7.fhir.dstu3.model.IdType;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author John Grimes
 */
@Service
public class DimensionResourceProvider implements IResourceProvider {

  Session session;

  @Autowired
  public DimensionResourceProvider(Session session) {
    this.session = session;
  }

  @Search()
  public List<Dimension> getAllDimensions() {
    return session.createQuery("SELECT d FROM Dimension d", Dimension.class).getResultList();
  }

  public Class<? extends IBaseResource> getResourceType() {
    return Dimension.class;
  }

  @Read()
  public Dimension getDimensionById(@IdParam IdType theId) {
    return session.byId(Dimension.class).load(theId.getIdPart());
  }

}
