/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.caching;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * @author John Grimes
 */
@Tag("UnitTest")
class CacheInvalidatorTest {

  @Test
  void invalidateAll() {
    final EntityTagValidator validator = mock(EntityTagValidator.class);
    final SparkSession spark = mock(SparkSession.class);
    final SQLContext sqlContext = mock(SQLContext.class);
    when(spark.sqlContext()).thenReturn(sqlContext);

    final CacheInvalidator invalidator = new CacheInvalidator(validator, spark);
    invalidator.invalidateAll();

    verify(validator).expire(anyLong());
    verify(sqlContext).clearCache();
  }

}
