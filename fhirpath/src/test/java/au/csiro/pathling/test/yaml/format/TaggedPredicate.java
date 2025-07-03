package au.csiro.pathling.test.yaml.format;

import au.csiro.pathling.test.yaml.YamlTestDefinition.TestCase;
import jakarta.annotation.Nonnull;
import jakarta.xml.bind.DatatypeConverter;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.function.Predicate;
import lombok.Value;

@Value(staticConstructor = "of")
class TaggedPredicate implements Predicate<TestCase> {

  @Nonnull
  Predicate<TestCase> predicate;

  @Nonnull
  String tag;

  @Override
  public boolean test(final TestCase testCase) {
    return predicate.test(testCase);
  }

  @Override
  @Nonnull
  public String toString() {
    return predicate + "#" + tag;
  }

  @Nonnull
  static TaggedPredicate of(@Nonnull final Predicate<TestCase> predicate,
      @Nonnull final String title, @Nonnull final String category) {
    try {
      // Create a MessageDigest instance for MD5
      final MessageDigest digest = MessageDigest.getInstance("MD5");
      // Update the digest with the bytes of the data
      final String data = category + title;
      final byte[] hashBytes = digest.digest(data.getBytes(StandardCharsets.UTF_8));
      // Convert the hash bytes to a hexadecimal string
      return TaggedPredicate.of(predicate,
          DatatypeConverter.printHexBinary(hashBytes).toLowerCase());
    } catch (final NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }
}
