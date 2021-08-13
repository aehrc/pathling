/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.io;

import static au.csiro.pathling.io.PersistenceScheme.convertS3ToS3aUrl;
import static au.csiro.pathling.io.PersistenceScheme.convertS3aToS3Url;

import au.csiro.pathling.Configuration;
import java.util.UUID;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * This class knows how to persist a Dataset of resources within a specified database.
 *
 * @author John Grimes
 */
@Component
@Profile("core")
public class ResultWriter {

  @Nonnull
  private final String resultUrl;

  /**
   * @param configuration A {@link Configuration} object which controls the behaviour of the writer
   */
  public ResultWriter(@Nonnull final Configuration configuration) {
    this.resultUrl = convertS3ToS3aUrl(configuration.getStorage().getResultUrl());
  }

  /**
   * Writes a result to the configured result storage area.
   *
   * @param result The {@link Dataset} containing the result.
   * @return the URL of the result
   */
  public String write(@Nonnull final Dataset result) {
    final String resultFileUrl = this.resultUrl + "/" + UUID.randomUUID() + ".csv";
    result.write()
        .mode(SaveMode.ErrorIfExists)
        .csv(resultFileUrl);
    //TODO: Merge partitioned files into one - possibly using FileUtil.copyMerge in Hadoop
    return convertS3aToS3Url(resultFileUrl);
  }

}
