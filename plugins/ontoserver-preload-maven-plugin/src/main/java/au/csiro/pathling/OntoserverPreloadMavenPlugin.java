/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import au.csiro.pathling.SyndicationFeed.Author;
import au.csiro.pathling.SyndicationFeed.Category;
import au.csiro.pathling.SyndicationFeed.Entry;
import au.csiro.pathling.SyndicationFeed.Link;
import com.thoughtworks.xstream.XStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates the preload data structure that Ontoserver expects from nominated upstream syndication
 * entries and/or local FHIR Bundles.
 *
 * @author John Grimes
 */
@Mojo(name = "assemble", defaultPhase = LifecyclePhase.GENERATE_RESOURCES)
public class OntoserverPreloadMavenPlugin extends AbstractMojo {

  private static final Logger logger = LoggerFactory.getLogger(OntoserverPreloadMavenPlugin.class);
  private static final String SUPPORTED_SYNDICATION_PROFILE = "http://ns.electronichealth.net.au/ncts/syndication/asf/profile/1.0.0";
  private final HttpClient httpClient;
  private final XStream xStream;

  @Parameter(defaultValue = "${project.build.directory}/ontoserver-preload")
  private String outputDirectory;

  @Parameter(property = "syndicationUrl", defaultValue = "https://api.healthterminologies.gov.au/syndication/v1/syndication.xml")
  private String syndicationUrl;

  @Parameter
  private List<ContentItemParameter> upstreamContentItems;

  @Parameter
  private List<String> bundles;

  public OntoserverPreloadMavenPlugin() {
    super();
    httpClient = HttpClients.createDefault();
    xStream = new XStream();
    XStream.setupDefaultSecurity(xStream);
    Class[] allowedClasses = new Class[]{SyndicationFeed.class, Entry.class, Link.class,
        Category.class, Author.class};
    xStream.allowTypes(allowedClasses);
    xStream.processAnnotations(SyndicationFeed.class);
    xStream.ignoreUnknownElements();
  }

  @Override
  public void execute() throws MojoExecutionException {
    try {
      // Ensure that the output directory exists.
      new File(outputDirectory).mkdirs();

      // Create a new preload feed.
      SyndicationFeed preloadFeed = new SyndicationFeed();
      preloadFeed.setAtomSyndicationFormatProfile(SUPPORTED_SYNDICATION_PROFILE);
      preloadFeed.setId("urn:uuid:" + UUID.randomUUID().toString());
      preloadFeed.setTitle("Ontoserver Preload Feed");
      preloadFeed.setEntries(new ArrayList<>());

      Link feedLink = new Link();
      feedLink.setRel("alternate");
      feedLink.setHref(preloadFeed.getId());
      preloadFeed.setLinks(Collections.singletonList(feedLink));

      // Add an updated date to the preload feed.
      String currentDate = Instant.now().toString();
      preloadFeed.setUpdated(currentDate);

      // If the `syndicationUrl` and `upstreamContentItems` parameters have been provided, get
      // upstream content items and add them to the preload feed.
      if (syndicationUrl != null && upstreamContentItems != null) {
        addUpstreamContentItemsToFeed(preloadFeed);
      }

      // If the `bundles` parameter has been provided, add local bundles to the feed.
      if (bundles != null) {
        addBundlesToFeed(preloadFeed);
      }

      String preloadFeedPath = outputDirectory + "/preload.xml";
      logger.info("Writing preload feed: " + preloadFeedPath);
      xStream.toXML(preloadFeed, new FileOutputStream(preloadFeedPath));
    } catch (Exception e) {
      throw new MojoExecutionException("Error occurred while executing plugin", e);
    }
  }

  private void addUpstreamContentItemsToFeed(SyndicationFeed preloadFeed)
      throws URISyntaxException, IOException {
    // Prepare a request to download the syndication feed.
    URI syndicationUri = new URI(syndicationUrl);
    HttpGet syndicationFeedRequest = new HttpGet(syndicationUri);
    syndicationFeedRequest.addHeader("Accept", "application/atom+xml");

    // Download the syndication feed.
    logger.info("Requesting syndication feed: " + syndicationUrl);
    InputStream syndicationResponseStream;
    SyndicationFeed upstreamFeed;
    try (CloseableHttpResponse response = (CloseableHttpResponse) httpClient
        .execute(syndicationFeedRequest)) {
      syndicationResponseStream = response.getEntity().getContent();
      upstreamFeed = (SyndicationFeed) xStream.fromXML(syndicationResponseStream);
    }

    // Check that the syndication feed declares the supported profile.
    if (!upstreamFeed.getAtomSyndicationFormatProfile()
        .equals(SUPPORTED_SYNDICATION_PROFILE)) {
      throw new RuntimeException("Syndication profile is not supported");
    }

    // Get matching entries from the upstream feed.
    List<Entry> matchingEntries = upstreamFeed.getEntries().stream()
        .filter(entry -> upstreamContentItems.stream()
            .anyMatch(ci -> {
              boolean identifierEqual = ci.getIdentifier().equals(entry.getContentItemIdentifier());
              boolean versionEqual = ci.getVersion().equals(entry.getContentItemVersion());
              boolean containsCategory = entry.getCategories().stream()
                  .anyMatch(category -> category.getTerm().equals(ci.getCategoryTerm()));
              return identifierEqual && versionEqual && containsCategory;
            }))
        .collect(Collectors.toList());

    // Add the upstream content items to the preload feed.
    preloadFeed.getEntries().addAll(matchingEntries);
  }

  private void addBundlesToFeed(SyndicationFeed preloadFeed) throws IOException {
    for (String bundlePath : bundles) {
      // Copy the bundle file into the output directory.
      File bundleFile = new File(bundlePath);
      String bundleFileName = bundleFile.getName();
      String bundleUpdated = Instant.ofEpochMilli(bundleFile.lastModified()).toString();
      String entryId = UUID.randomUUID().toString();
      File targetFile = new File(outputDirectory + "/" + bundleFileName);
      logger
          .info("Copying: " + bundleFile.getAbsolutePath() + " -> " + targetFile.getAbsolutePath());
      Files.copy(bundleFile.toPath(), targetFile.toPath(), REPLACE_EXISTING);

      // Create a new entry for the preload feed that refers to the bundle file.
      Entry bundleEntry = new Entry();
      bundleEntry.setId("urn:uuid:" + entryId);
      bundleEntry.setUpdated(bundleUpdated);
      bundleEntry.setTitle(bundleFileName);
      bundleEntry.setBundleInterpretation("batch");
      bundleEntry.setContentItemIdentifier(entryId);
      bundleEntry.setContentItemVersion("1.0.0");

      Category bundleCategory = new Category();
      bundleCategory.setTerm("FHIR_BUNDLE");
      bundleCategory.setLabel("FHIR Bundle");
      bundleCategory
          .setScheme("http://ns.electronichealth.net.au/ncts/syndication/asf/scheme/1.0.0");
      bundleEntry.setCategories(Collections.singletonList(bundleCategory));

      Link bundleLink = new Link();
      bundleLink.setRel("alternate");
      bundleLink.setHref("file:///data/" + bundleFileName);
      bundleLink.setType("application/fhir+json");
      bundleEntry.setLinks(Collections.singletonList(bundleLink));

      Author bundleAuthor = new Author();
      bundleAuthor.setName("ontoserver-preload-maven-plugin");
      bundleEntry.setAuthors(Collections.singletonList(bundleAuthor));

      // Add the bundle entry to the preload feed.
      preloadFeed.getEntries().add(bundleEntry);
    }
  }

}
