/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.terminology.local;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Resolves the default (latest) version from a set of candidate versions of the same code system,
 * replicating the version-ordering behaviour of the reference terminology server (Ontoserver's
 * {@code FhirResourceUtils}). This ensures that Pathling's local mode selects exactly the same
 * default version as the reference server used for differential testing.
 *
 * <p>The dispatch mirrors the reference: SNOMED CT URIs use edition and effectiveTime ordering with
 * a configurable default edition; any eight-digit date version uses date ordering (with {@code
 * MMDDYYYY} transposed to {@code YYYYMMDD}); a set that is entirely SemVer uses SemVer 2.0.0
 * precedence; otherwise a heuristic segment comparison is used. Genuinely undecidable cases raise
 * an {@link AmbiguousVersionException}.
 *
 * @author John Grimes
 */
public class VersionResolver {

  private static final String URI_SCT = "http://snomed.info/sct";

  private static final Pattern DATE_VERSIONING = Pattern.compile("^([0-9]{8})$");

  private static final Pattern US_DATE_VERSIONING =
      Pattern.compile("^(?<monthDay>((0[1-9])|(1[012]))[0-3][0-9])(?<year>[0-9]{4})$");

  private static final Pattern SEMVER_FORMAT =
      Pattern.compile(
          "^(?<major>0|[1-9]\\d*)\\.(?<minor>0|[1-9]\\d*)\\.(?<patch>0|[1-9]\\d*)"
              + "(?:-(?<prerelease>[\\da-zA-Z-]++(?:\\.[\\da-zA-Z-]++)*+))?"
              + "(?:\\+(?<build>[\\da-zA-Z-]++(?:\\.[\\da-zA-Z-]++)*+))?$");

  private static final Pattern SNOMED_VERSION_PATTERN =
      Pattern.compile(
          "^http://snomed.info/(?<experimental>x?)sct/(?<edition>[1-9]\\d{5,17})"
              + "/version/(?<effectiveTime>\\d{8})");

  @Nullable private final String defaultSnomedEdition;

  /**
   * Creates a resolver.
   *
   * @param defaultSnomedEdition the SNOMED CT module identifier of the edition to prefer for
   *     unversioned SNOMED references, or null if no default edition is configured
   */
  public VersionResolver(@Nullable final String defaultSnomedEdition) {
    this.defaultSnomedEdition = defaultSnomedEdition;
  }

  /**
   * Returns the item whose version is the latest, according to the ordering rules for the given
   * code system URL.
   *
   * @param items the candidate items
   * @param getKey extracts the version string from an item
   * @param url the canonical URL of the code system the items belong to
   * @param <T> the item type
   * @return the item with the latest version, or null if {@code items} is empty
   * @throws AmbiguousVersionException if no single latest version can be determined
   */
  @Nullable
  public <T> T getLatestOfVersions(
      @Nonnull final List<T> items,
      @Nonnull final Function<T, String> getKey,
      @Nonnull final String url) {
    if (items.isEmpty()) {
      return null;
    } else if (items.size() == 1) {
      return items.get(0);
    } else if (items.stream().anyMatch(item -> isEmpty(getKey.apply(item)))) {
      throw new AmbiguousVersionException(
          "Found more than one resource with the URL "
              + url
              + " and was unable to determine a default version; a version was missing: "
              + items);
    }

    if (URI_SCT.equals(url)) {
      return getLatestOfSnomedVersions(items, getKey, url);
    } else if (items.stream()
        .anyMatch(item -> DATE_VERSIONING.matcher(getKey.apply(item)).matches())) {
      return getLatestOfDateVersions(items, getKey, url);
    } else if (items.stream()
        .allMatch(item -> SEMVER_FORMAT.matcher(getKey.apply(item)).matches())) {
      return getLatestOfSemVers(items, getKey, url);
    }
    return guessLatestOfVersions(items, getKey, url);
  }

  @Nullable
  private <T> T getLatestOfSnomedVersions(
      @Nonnull final List<T> items,
      @Nonnull final Function<T, String> getKey,
      @Nonnull final String url) {
    final Set<String> editions = new HashSet<>();
    final List<T> defaultEditionItems = new ArrayList<>();
    for (final T item : items) {
      final String key = getKey.apply(item);
      final Matcher matcher = SNOMED_VERSION_PATTERN.matcher(key);
      if (!matcher.matches()) {
        throw new AmbiguousVersionException(
            "Invalid version '" + key + "' for SNOMED CT code system with URL " + url);
      }
      final String edition = matcher.group("edition");
      editions.add(edition);
      if (edition.equals(defaultSnomedEdition)) {
        defaultEditionItems.add(item);
      }
    }
    final List<T> candidates;
    if (!defaultEditionItems.isEmpty()) {
      candidates = defaultEditionItems;
    } else if (editions.size() > 1) {
      throw new AmbiguousVersionException(
          "Found more than one SNOMED CT edition for the URL "
              + url
              + " and no default edition is configured; stored editions: "
              + editions);
    } else {
      candidates = items;
    }

    // Experimental (xsct) versions sort lower than production releases; otherwise compare the full
    // version URI (its trailing effectiveTime governs the ordering within an edition).
    final Comparator<String> comparator =
        (a, b) -> {
          final boolean aExperimental = a.contains("/xsct/");
          final boolean bExperimental = b.contains("/xsct/");
          if (aExperimental && !bExperimental) {
            return -1;
          }
          if (bExperimental && !aExperimental) {
            return 1;
          }
          return a.compareTo(b);
        };

    T latest = null;
    String latestKey = null;
    boolean duplicateLatest = false;
    for (final T item : candidates) {
      final String key = getKey.apply(item);
      if (latest == null || comparator.compare(key, latestKey) > 0) {
        latest = item;
        latestKey = key;
        duplicateLatest = false;
      } else if (key.equals(latestKey)) {
        duplicateLatest = true;
      }
    }
    if (duplicateLatest) {
      throw new AmbiguousVersionException(
          "Found more than one resource with the URL "
              + url
              + " and the same version: "
              + latestKey);
    }
    return latest;
  }

  @Nullable
  private static <T> T getLatestOfDateVersions(
      @Nonnull final List<T> items,
      @Nonnull final Function<T, String> getKey,
      @Nonnull final String url) {
    T latest = null;
    int latestVersion = 0;
    boolean duplicateLatest = false;
    for (final T item : items) {
      final String rawKey = getKey.apply(item);
      final String key;
      final Matcher usMatcher = US_DATE_VERSIONING.matcher(rawKey);
      if (usMatcher.matches()) {
        // Transpose MMDDYYYY to YYYYMMDD for comparison.
        key = usMatcher.group("year") + usMatcher.group("monthDay");
      } else {
        key = rawKey;
      }
      final Matcher matcher = DATE_VERSIONING.matcher(key);
      if (matcher.matches()) {
        final int version = Integer.parseInt(matcher.group());
        if (latest != null && version == latestVersion) {
          duplicateLatest = true;
        }
        if (latest == null || version > latestVersion) {
          latest = item;
          latestVersion = version;
          duplicateLatest = false;
        }
      } else {
        throw new AmbiguousVersionException(
            "Found more than one resource with the URL "
                + url
                + " with date-like versions except for "
                + rawKey
                + "; unable to determine a default version: "
                + items);
      }
    }
    if (duplicateLatest) {
      throw new AmbiguousVersionException(
          "Found more than one resource with the URL "
              + url
              + " and the same version: "
              + latestVersion);
    }
    return latest;
  }

  @Nullable
  private static <T> T getLatestOfSemVers(
      @Nonnull final List<T> items,
      @Nonnull final Function<T, String> getKey,
      @Nonnull final String url) {
    T latest = null;
    Matcher latestMatch = null;
    boolean duplicateLatest = false;
    for (final T item : items) {
      final String key = getKey.apply(item);
      final Matcher matcher = SEMVER_FORMAT.matcher(key);
      if (!matcher.matches()) {
        throw new AmbiguousVersionException(
            "Found more than one resource with the URL "
                + url
                + " with SemVer-like versions except for "
                + key
                + "; unable to determine a default version: "
                + items);
      }
      final int comparison = latestMatch == null ? 1 : compareSemVer(matcher, latestMatch);
      if (comparison == 0) {
        duplicateLatest = true;
      } else if (comparison > 0) {
        latest = item;
        latestMatch = matcher;
        duplicateLatest = false;
      }
    }
    if (duplicateLatest) {
      throw new AmbiguousVersionException(
          "Found more than one resource with the URL "
              + url
              + " and the same version: "
              + getKey.apply(latest));
    }
    return latest;
  }

  /**
   * Compares two SemVer matches by SemVer 2.0.0 precedence: major.minor.patch, then pre-release
   * (absent outranks present), then pre-release identifiers left-to-right. Build metadata is
   * ignored.
   */
  private static int compareSemVer(@Nonnull final Matcher a, @Nonnull final Matcher b) {
    for (final String group : new String[] {"major", "minor", "patch"}) {
      final int comparison =
          Integer.compare(Integer.parseInt(a.group(group)), Integer.parseInt(b.group(group)));
      if (comparison != 0) {
        return comparison;
      }
    }
    final String aPre = a.group("prerelease");
    final String bPre = b.group("prerelease");
    if (aPre == null && bPre == null) {
      return 0;
    } else if (aPre == null) {
      return 1;
    } else if (bPre == null) {
      return -1;
    }
    final String[] aParts = aPre.split("\\.");
    final String[] bParts = bPre.split("\\.");
    for (int i = 0; i < Math.min(aParts.length, bParts.length); i++) {
      final Integer aNum = parseIntOrNull(aParts[i]);
      final Integer bNum = parseIntOrNull(bParts[i]);
      if (aNum != null && bNum != null) {
        final int comparison = aNum.compareTo(bNum);
        if (comparison != 0) {
          return comparison;
        }
      } else if (aNum != null) {
        return -1;
      } else if (bNum != null) {
        return 1;
      } else {
        final int comparison = aParts[i].compareTo(bParts[i]);
        if (comparison != 0) {
          return comparison;
        }
      }
    }
    return Integer.compare(aParts.length, bParts.length);
  }

  @Nullable
  private static Integer parseIntOrNull(@Nonnull final String s) {
    if (s.length() > 1 && s.charAt(0) == '0') {
      // Leading-zero identifiers are alphanumeric per SemVer §9.
      return null;
    }
    try {
      return Integer.valueOf(s);
    } catch (final NumberFormatException e) {
      return null;
    }
  }

  @Nullable
  private static <T> T guessLatestOfVersions(
      @Nonnull final List<T> items,
      @Nonnull final Function<T, String> getKey,
      @Nonnull final String url) {
    T latest = null;
    String[] latestVersion = {};
    boolean duplicateLatest = false;

    for (final T item : items) {
      final String key = getKey.apply(item);

      duplicateLatest = true;
      final String[] thisVersion = splitVersion(key);
      final int len = Math.max(latestVersion.length, thisVersion.length);
      for (int i = 0; i < len; i++) {
        final String s0 = i < latestVersion.length ? latestVersion[i] : null;
        final String s1 = i < thisVersion.length ? thisVersion[i] : null;
        final boolean numeric =
            (s0 != null && Character.isDigit(s0.charAt(0)))
                || (s1 != null && Character.isDigit(s1.charAt(0)));

        if (numeric) {
          try {
            final int i0 = s0 == null ? 0 : Integer.parseInt(s0);
            final int i1 = s1 == null ? 0 : Integer.parseInt(s1);
            if (i1 > i0) {
              latestVersion = thisVersion;
              latest = item;
              duplicateLatest = false;
              break;
            } else if (i1 < i0) {
              duplicateLatest = false;
              break;
            }
          } catch (final NumberFormatException e) {
            throw new AmbiguousVersionException(
                "Could not compare version segments as numbers: " + s0 + " and " + s1);
          }
        } else {
          if (s0 == null || (s1 != null && s0.compareTo(s1) < 0)) {
            latestVersion = thisVersion;
            latest = item;
            duplicateLatest = false;
            break;
          } else if (!s0.equals(s1)) {
            duplicateLatest = false;
            break;
          }
        }
      }
      // Trailing-numeric-zero tiebreaker: when two versions compare equal only because the shorter
      // zero-pads to match the longer, prefer the more explicit (longer) form.
      if (duplicateLatest && thisVersion.length != latestVersion.length) {
        if (thisVersion.length > latestVersion.length
            && trailingExtrasAreNumericZeros(thisVersion, latestVersion.length)) {
          latestVersion = thisVersion;
          latest = item;
          duplicateLatest = false;
        } else if (thisVersion.length < latestVersion.length
            && trailingExtrasAreNumericZeros(latestVersion, thisVersion.length)) {
          duplicateLatest = false;
        }
      }
    }
    if (duplicateLatest) {
      throw new AmbiguousVersionException(
          "Found more than one resource with the URL "
              + url
              + " and the same version segments: "
              + String.join(" ", latestVersion));
    }
    return latest;
  }

  /**
   * Returns true if the two version strings would be regarded as the same canonical version by the
   * latest-version resolver, that is, equal segment by segment with implicit zero-padding for
   * trailing positions. Used to deduplicate versions at import time.
   *
   * @param v1 the first version string
   * @param v2 the second version string
   * @return true if the versions are equivalent
   */
  public static boolean versionsAreEquivalent(
      @Nullable final String v1, @Nullable final String v2) {
    if (v1 == null || v2 == null) {
      return v1 == null && v2 == null;
    }
    if (v1.equals(v2)) {
      return true;
    }
    final String[] a = splitVersion(v1);
    final String[] b = splitVersion(v2);
    final int len = Math.max(a.length, b.length);
    for (int i = 0; i < len; i++) {
      final String s0 = i < a.length ? a[i] : null;
      final String s1 = i < b.length ? b[i] : null;
      final boolean numeric =
          (s0 != null && Character.isDigit(s0.charAt(0)))
              || (s1 != null && Character.isDigit(s1.charAt(0)));
      if (numeric) {
        try {
          final int i0 = s0 == null ? 0 : Integer.parseInt(s0);
          final int i1 = s1 == null ? 0 : Integer.parseInt(s1);
          if (i0 != i1) {
            return false;
          }
        } catch (final NumberFormatException e) {
          return false;
        }
      } else {
        if (s0 == null || s1 == null || !s0.equals(s1)) {
          return false;
        }
      }
    }
    return true;
  }

  private static boolean trailingExtrasAreNumericZeros(
      @Nonnull final String[] longerVersion, final int shorterLength) {
    for (int i = shorterLength; i < longerVersion.length; i++) {
      final String s = longerVersion[i];
      if (s.isEmpty() || !Character.isDigit(s.charAt(0))) {
        return false;
      }
      try {
        if (Integer.parseInt(s) != 0) {
          return false;
        }
      } catch (final NumberFormatException e) {
        return false;
      }
    }
    return true;
  }

  @Nonnull
  private static String[] splitVersion(@Nonnull final String version) {
    final List<String> segments = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    boolean digitState = false;

    for (final char c : version.toCharArray()) {
      if (c == '.' || c == '-' || c == '_' || c == '/' || c == ';' || c == ':') {
        if (current.length() > 0) {
          segments.add(current.toString());
          current = new StringBuilder();
        }
        continue;
      }
      if (current.length() == 0) {
        digitState = Character.isDigit(c);
      } else if (digitState != Character.isDigit(c)) {
        digitState = !digitState;
        segments.add(current.toString());
        current = new StringBuilder();
      }
      current.append(c);
    }
    if (current.length() > 0) {
      segments.add(current.toString());
    }
    return segments.toArray(new String[0]);
  }

  private static boolean isEmpty(@Nullable final String s) {
    return s == null || s.isEmpty();
  }
}
