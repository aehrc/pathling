/*
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight.loaders;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

/**
 * @author John Grimes
 */
@Mojo(name = "load")
public class LoincLoaderMojo extends AbstractMojo {

  @Parameter(property = "jdbcUrl", required = true)
  private String jdbcUrl;

  @Parameter(property = "jdbcDriver", required = true)
  private String jdbcDriver;

  @Parameter(property = "terminologyServerUrl", required = true)
  private String terminologyServerUrl;

  @Parameter(property = "loincVersion")
  private String loincVersion;

  @Parameter(property = "loincUrl", defaultValue = "http://loinc.org")
  private String loincUrl;

  @Parameter(property = "autoDdl", defaultValue = "update")
  private String autoDdl;

  @Parameter(property = "expansionPageSize", defaultValue = "2000")
  private String expansionPageSize;

  @Override
  public void execute() throws MojoExecutionException {
    try {
      LoincLoader loincLoader = new LoincLoader(jdbcUrl,
          terminologyServerUrl,
          jdbcDriver,
          autoDdl);
      loincLoader.execute(loincUrl, loincVersion, Integer.parseInt(expansionPageSize));
    } catch (Exception e) {
      throw new MojoExecutionException("Error occurred during execution", e);
    }
  }

}
