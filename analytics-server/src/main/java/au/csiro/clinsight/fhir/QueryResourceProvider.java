/*
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight.fhir;

import au.csiro.clinsight.persistence.Query;
import ca.uhn.fhir.rest.annotation.Search;
import ca.uhn.fhir.rest.server.IResourceProvider;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * @author John Grimes
 */
@Service
public class QueryResourceProvider implements IResourceProvider {

    public Class<? extends IBaseResource> getResourceType() {
        return Query.class;
    }

    @Search()
    public List<Query> getAllQueries() {
        return new ArrayList<Query>();
    }

}
