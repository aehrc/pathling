package au.csiro.pathling.operations.bulkimport;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for ImportResult record class.
 *
 * @author John Grimes
 */
class ImportResultTest {

  @Test
  void canCreateImportResultWithOwnerId() {
    // Given
    final String ownerId = "user123";

    // When
    final ImportResult result = new ImportResult(Optional.of(ownerId));

    // Then
    assertThat(result.ownerId()).isPresent();
    assertThat(result.ownerId()).contains(ownerId);
  }

  @Test
  void canCreateImportResultWithoutOwnerId() {
    // When
    final ImportResult result = new ImportResult(Optional.empty());

    // Then
    assertThat(result.ownerId()).isEmpty();
  }

  @Test
  void recordEqualityBasedOnOwnerId() {
    // Given
    final ImportResult result1 = new ImportResult(Optional.of("user123"));
    final ImportResult result2 = new ImportResult(Optional.of("user123"));
    final ImportResult result3 = new ImportResult(Optional.of("user456"));

    // When/Then
    assertThat(result1).isEqualTo(result2);
    assertThat(result1).isNotEqualTo(result3);
  }

  @Test
  void recordEqualityWithEmptyOwner() {
    // Given
    final ImportResult result1 = new ImportResult(Optional.empty());
    final ImportResult result2 = new ImportResult(Optional.empty());

    // When/Then
    assertThat(result1).isEqualTo(result2);
  }

  @Test
  void recordHashCodeConsistency() {
    // Given
    final ImportResult result1 = new ImportResult(Optional.of("user123"));
    final ImportResult result2 = new ImportResult(Optional.of("user123"));

    // When/Then
    assertThat(result1.hashCode()).isEqualTo(result2.hashCode());
  }

  @Test
  void recordToStringIncludesOwnerId() {
    // Given
    final ImportResult result = new ImportResult(Optional.of("user123"));

    // When
    final String toString = result.toString();

    // Then
    assertThat(toString).contains("ImportResult");
    assertThat(toString).contains("user123");
  }

  @Test
  void recordToStringWithEmptyOwnerId() {
    // Given
    final ImportResult result = new ImportResult(Optional.empty());

    // When
    final String toString = result.toString();

    // Then
    assertThat(toString).contains("ImportResult");
  }
}
