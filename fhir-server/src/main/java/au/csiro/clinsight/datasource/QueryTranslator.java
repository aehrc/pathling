/**
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use
 * is subject to license terms and conditions.
 */

package au.csiro.clinsight.datasource;

import au.csiro.clinsight.resources.AggregateQuery;

public interface QueryTranslator<T> {

  T translateQuery(AggregateQuery query);

}
