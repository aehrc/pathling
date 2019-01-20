/*
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight.fhir;

import au.csiro.clinsight.persistence.DimensionAttribute;
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
public class DimensionAttributeResourceProvider implements IResourceProvider {

  Session session;

  @Autowired
  public DimensionAttributeResourceProvider(Session session) {
    this.session = session;
  }

  @Search()
  public List<DimensionAttribute> getAllDimensionAttributes() {
    return session.createQuery("SELECT da FROM DimensionAttribute da", DimensionAttribute.class)
        .getResultList();
  }

  @Override
  public Class<? extends IBaseResource> getResourceType() {
    return DimensionAttribute.class;
  }

  @Read()
  public DimensionAttribute getDimensionAttributeById(@IdParam IdType theId) {
    return session.byId(DimensionAttribute.class).load(theId.getIdPart());
  }

}
