/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamAsAttribute;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import java.util.List;

/**
 * Used for serializing and deserializing syndication feeds.
 *
 * @author John Grimes
 */
@XStreamAlias("feed")
public class SyndicationFeed {

  private String id;
  private String title;
  private String updated;

  @XStreamAsAttribute
  private String xmlns = "http://www.w3.org/2005/Atom";

  @XStreamAsAttribute
  @XStreamAlias("xmlns:ncts")
  private String xmlnsNcts = "http://ns.electronichealth.net.au/ncts/syndication/asf/extensions/1.0.0";

  @XStreamAlias("ncts:atomSyndicationFormatProfile")
  private String atomSyndicationFormatProfile;

  @XStreamImplicit(itemFieldName = "link")
  private List<Link> links;

  @XStreamImplicit(itemFieldName = "entry")
  private List<Entry> entries;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public String getUpdated() {
    return updated;
  }

  public void setUpdated(String updated) {
    this.updated = updated;
  }

  public List<Link> getLinks() {
    return links;
  }

  public void setLinks(List<Link> links) {
    this.links = links;
  }

  public String getAtomSyndicationFormatProfile() {
    return atomSyndicationFormatProfile;
  }

  public void setAtomSyndicationFormatProfile(String atomSyndicationFormatProfile) {
    this.atomSyndicationFormatProfile = atomSyndicationFormatProfile;
  }

  public List<Entry> getEntries() {
    return entries;
  }

  public void setEntries(List<Entry> entries) {
    this.entries = entries;
  }

  public static class Entry {

    @XStreamAlias("ncts:contentItemIdentifier")
    private String contentItemIdentifier;

    @XStreamAlias("ncts:contentItemVersion")
    private String contentItemVersion;

    @XStreamAlias("ncts:bundleInterpretation")
    private String bundleInterpretation;

    @XStreamImplicit(itemFieldName = "link")
    private List<Link> links;

    @XStreamImplicit(itemFieldName = "category")
    private List<Category> categories;

    @XStreamImplicit(itemFieldName = "author")
    private List<Author> authors;

    private String title;
    private String id;
    private String updated;

    public String getContentItemIdentifier() {
      return contentItemIdentifier;
    }

    public void setContentItemIdentifier(String contentItemIdentifier) {
      this.contentItemIdentifier = contentItemIdentifier;
    }

    public String getContentItemVersion() {
      return contentItemVersion;
    }

    public void setContentItemVersion(String contentItemVersion) {
      this.contentItemVersion = contentItemVersion;
    }

    public String getBundleInterpretation() {
      return bundleInterpretation;
    }

    public void setBundleInterpretation(String bundleInterpretation) {
      this.bundleInterpretation = bundleInterpretation;
    }

    public List<Link> getLinks() {
      return links;
    }

    public void setLinks(List<Link> links) {
      this.links = links;
    }

    public List<Category> getCategories() {
      return categories;
    }

    public void setCategories(List<Category> categories) {
      this.categories = categories;
    }

    public List<Author> getAuthors() {
      return authors;
    }

    public void setAuthors(List<Author> authors) {
      this.authors = authors;
    }

    public String getTitle() {
      return title;
    }

    public void setTitle(String title) {
      this.title = title;
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public String getUpdated() {
      return updated;
    }

    public void setUpdated(String updated) {
      this.updated = updated;
    }

  }

  public static class Link {

    @XStreamAsAttribute
    private String rel;

    @XStreamAsAttribute
    private String type;

    @XStreamAsAttribute
    private String href;

    @XStreamAlias("ncts:sha256Hash")
    @XStreamAsAttribute
    private String sha256Hash;

    public String getRel() {
      return rel;
    }

    public void setRel(String rel) {
      this.rel = rel;
    }

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public String getHref() {
      return href;
    }

    public void setHref(String href) {
      this.href = href;
    }

    public String getSha256Hash() {
      return sha256Hash;
    }

    public void setSha256Hash(String sha256Hash) {
      this.sha256Hash = sha256Hash;
    }

  }

  public static class Category {

    @XStreamAsAttribute
    private String term;

    @XStreamAsAttribute
    private String label;

    @XStreamAsAttribute
    private String scheme;

    public String getTerm() {
      return term;
    }

    public void setTerm(String term) {
      this.term = term;
    }

    public String getLabel() {
      return label;
    }

    public void setLabel(String label) {
      this.label = label;
    }

    public String getScheme() {
      return scheme;
    }

    public void setScheme(String scheme) {
      this.scheme = scheme;
    }

  }

  public static class Author {

    private String name;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

  }

}
