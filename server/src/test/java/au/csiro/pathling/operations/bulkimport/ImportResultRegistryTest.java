package au.csiro.pathling.operations.bulkimport;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for ImportResultRegistry.
 *
 * @author John Grimes
 */
class ImportResultRegistryTest {

  private ImportResultRegistry registry;

  @BeforeEach
  void setUp() {
    registry = new ImportResultRegistry();
  }

  // ========================================
  // Put and Get Tests
  // ========================================

  @Test
  void putAndGetResult() {
    // Given
    final String jobId = "job123";
    final ImportResult result = new ImportResult(Optional.of("user123"));

    // When
    registry.put(jobId, result);

    // Then
    assertThat(registry.get(jobId)).isPresent();
    assertThat(registry.get(jobId)).contains(result);
  }

  @Test
  void putReturnsRegistryForChaining() {
    // Given
    final String jobId = "job123";
    final ImportResult result = new ImportResult(Optional.of("user123"));

    // When
    final ImportResultRegistry returned = registry.put(jobId, result);

    // Then
    assertThat(returned).isSameAs(registry);
  }

  @Test
  void putAndReturnReturnsResult() {
    // Given
    final String jobId = "job123";
    final ImportResult result = new ImportResult(Optional.of("user123"));

    // When
    final ImportResult returned = registry.putAndReturn(jobId, result);

    // Then
    assertThat(returned).isSameAs(result);
    assertThat(registry.get(jobId)).contains(result);
  }

  @Test
  void getReturnsEmptyForNonExistentJob() {
    // When
    final Optional<ImportResult> result = registry.get("nonexistent");

    // Then
    assertThat(result).isEmpty();
  }

  // ========================================
  // Outcome Tests
  // ========================================

  @Test
  void addOutcomeStoresOutcome() {
    // Given
    final String jobId = "job123";
    final OperationOutcome outcome = createOutcome("Test message", IssueSeverity.INFORMATION);

    // When
    registry.addOutcome(jobId, outcome);

    // Then
    assertThat(registry.getOutcomes(jobId)).hasSize(1);
    assertThat(registry.getOutcomes(jobId)).contains(outcome);
  }

  @Test
  void addOutcomeReturnsRegistryForChaining() {
    // Given
    final String jobId = "job123";
    final OperationOutcome outcome = createOutcome("Test message", IssueSeverity.INFORMATION);

    // When
    final ImportResultRegistry returned = registry.addOutcome(jobId, outcome);

    // Then
    assertThat(returned).isSameAs(registry);
  }

  @Test
  void addOutcomeAndReturnReturnsOutcome() {
    // Given
    final String jobId = "job123";
    final OperationOutcome outcome = createOutcome("Test message", IssueSeverity.INFORMATION);

    // When
    final OperationOutcome returned = registry.addOutcomeAndReturn(jobId, outcome);

    // Then
    assertThat(returned).isSameAs(outcome);
    assertThat(registry.getOutcomes(jobId)).contains(outcome);
  }

  @Test
  void addOutcomesStoresMultipleOutcomes() {
    // Given
    final String jobId = "job123";
    final OperationOutcome outcome1 = createOutcome("Message 1", IssueSeverity.INFORMATION);
    final OperationOutcome outcome2 = createOutcome("Message 2", IssueSeverity.WARNING);

    // When
    registry.addOutcomes(jobId, List.of(outcome1, outcome2));

    // Then
    assertThat(registry.getOutcomes(jobId)).hasSize(2);
    assertThat(registry.getOutcomes(jobId)).containsExactly(outcome1, outcome2);
  }

  @Test
  void addOutcomesReturnsRegistryForChaining() {
    // Given
    final String jobId = "job123";
    final OperationOutcome outcome = createOutcome("Test message", IssueSeverity.INFORMATION);

    // When
    final ImportResultRegistry returned = registry.addOutcomes(jobId, List.of(outcome));

    // Then
    assertThat(returned).isSameAs(registry);
  }

  @Test
  void addMultipleOutcomesToSameJob() {
    // Given
    final String jobId = "job123";
    final OperationOutcome outcome1 = createOutcome("Message 1", IssueSeverity.INFORMATION);
    final OperationOutcome outcome2 = createOutcome("Message 2", IssueSeverity.WARNING);

    // When
    registry.addOutcome(jobId, outcome1);
    registry.addOutcome(jobId, outcome2);

    // Then
    assertThat(registry.getOutcomes(jobId)).hasSize(2);
    assertThat(registry.getOutcomes(jobId)).containsExactly(outcome1, outcome2);
  }

  @Test
  void getOutcomesReturnsEmptyForNonExistentJob() {
    // When
    final List<OperationOutcome> outcomes = registry.getOutcomes("nonexistent");

    // Then
    assertThat(outcomes).isEmpty();
  }

  @Test
  void getOutcomesReturnsImmutableList() {
    // Given
    final String jobId = "job123";
    final OperationOutcome outcome = createOutcome("Test message", IssueSeverity.INFORMATION);
    registry.addOutcome(jobId, outcome);

    // When
    final List<OperationOutcome> outcomes = registry.getOutcomes(jobId);

    // Then - verify list is unmodifiable by attempting to modify it
    assertThat(outcomes).hasSize(1);
    try {
      outcomes.add(createOutcome("Another message", IssueSeverity.INFORMATION));
      assertThat(false).as("Expected UnsupportedOperationException").isTrue();
    } catch (final UnsupportedOperationException e) {
      // Expected
    }
  }

  // ========================================
  // GetAll Tests
  // ========================================

  @Test
  void getAllResultsReturnsAllStoredResults() {
    // Given
    final ImportResult result1 = new ImportResult(Optional.of("user1"));
    final ImportResult result2 = new ImportResult(Optional.of("user2"));
    registry.put("job1", result1);
    registry.put("job2", result2);

    // When
    final Map<String, ImportResult> allResults = registry.getAllResults();

    // Then
    assertThat(allResults).hasSize(2);
    assertThat(allResults).containsEntry("job1", result1);
    assertThat(allResults).containsEntry("job2", result2);
  }

  @Test
  void getAllResultsReturnsEmptyWhenNoResults() {
    // When
    final Map<String, ImportResult> allResults = registry.getAllResults();

    // Then
    assertThat(allResults).isEmpty();
  }

  @Test
  void getAllResultsReturnsImmutableMap() {
    // Given
    registry.put("job1", new ImportResult(Optional.of("user1")));

    // When
    final Map<String, ImportResult> allResults = registry.getAllResults();

    // Then - verify map is unmodifiable by attempting to modify it
    assertThat(allResults).hasSize(1);
    try {
      allResults.put("job2", new ImportResult(Optional.of("user2")));
      assertThat(false).as("Expected UnsupportedOperationException").isTrue();
    } catch (final UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test
  void getAllOutcomesReturnsAllStoredOutcomes() {
    // Given
    final OperationOutcome outcome1 = createOutcome("Message 1", IssueSeverity.INFORMATION);
    final OperationOutcome outcome2 = createOutcome("Message 2", IssueSeverity.WARNING);
    registry.addOutcome("job1", outcome1);
    registry.addOutcome("job2", outcome2);

    // When
    final Map<String, List<OperationOutcome>> allOutcomes = registry.getAllOutcomes();

    // Then
    assertThat(allOutcomes).hasSize(2);
    assertThat(allOutcomes.get("job1")).containsExactly(outcome1);
    assertThat(allOutcomes.get("job2")).containsExactly(outcome2);
  }

  @Test
  void getAllOutcomesReturnsEmptyWhenNoOutcomes() {
    // When
    final Map<String, List<OperationOutcome>> allOutcomes = registry.getAllOutcomes();

    // Then
    assertThat(allOutcomes).isEmpty();
  }

  @Test
  void getAllOutcomesReturnsImmutableMap() {
    // Given
    registry.addOutcome("job1", createOutcome("Message", IssueSeverity.INFORMATION));

    // When
    final Map<String, List<OperationOutcome>> allOutcomes = registry.getAllOutcomes();

    // Then - verify map is unmodifiable by attempting to modify it
    assertThat(allOutcomes).hasSize(1);
    try {
      allOutcomes.put("job2", List.of(createOutcome("Message", IssueSeverity.INFORMATION)));
      assertThat(false).as("Expected UnsupportedOperationException").isTrue();
    } catch (final UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test
  void getAllOutcomesReturnsImmutableLists() {
    // Given
    registry.addOutcome("job1", createOutcome("Message", IssueSeverity.INFORMATION));

    // When
    final Map<String, List<OperationOutcome>> allOutcomes = registry.getAllOutcomes();

    // Then - verify list is unmodifiable by attempting to modify it
    final List<OperationOutcome> list = allOutcomes.get("job1");
    assertThat(list).hasSize(1);
    try {
      list.add(createOutcome("Another message", IssueSeverity.INFORMATION));
      assertThat(false).as("Expected UnsupportedOperationException").isTrue();
    } catch (final UnsupportedOperationException e) {
      // Expected
    }
  }

  // ========================================
  // Remove Tests
  // ========================================

  @Test
  void removeDeletesResultAndOutcomes() {
    // Given
    final String jobId = "job123";
    final ImportResult result = new ImportResult(Optional.of("user123"));
    final OperationOutcome outcome = createOutcome("Test message", IssueSeverity.INFORMATION);
    registry.put(jobId, result);
    registry.addOutcome(jobId, outcome);

    // When
    final ImportResult removed = registry.remove(jobId);

    // Then
    assertThat(removed).isEqualTo(result);
    assertThat(registry.get(jobId)).isEmpty();
    assertThat(registry.getOutcomes(jobId)).isEmpty();
  }

  @Test
  void removeReturnsNullForNonExistentJob() {
    // When
    final ImportResult removed = registry.remove("nonexistent");

    // Then
    assertThat(removed).isNull();
  }

  // ========================================
  // ContainsKey Tests
  // ========================================

  @Test
  void containsKeyReturnsTrueForExistingJob() {
    // Given
    final String jobId = "job123";
    registry.put(jobId, new ImportResult(Optional.of("user123")));

    // When/Then
    assertThat(registry.containsKey(jobId)).isTrue();
  }

  @Test
  void containsKeyReturnsFalseForNonExistentJob() {
    // When/Then
    assertThat(registry.containsKey("nonexistent")).isFalse();
  }

  // ========================================
  // Clear Tests
  // ========================================

  @Test
  void clearRemovesAllResultsAndOutcomes() {
    // Given
    registry.put("job1", new ImportResult(Optional.of("user1")));
    registry.put("job2", new ImportResult(Optional.of("user2")));
    registry.addOutcome("job1", createOutcome("Message", IssueSeverity.INFORMATION));

    // When
    registry.clear();

    // Then
    assertThat(registry.isEmpty()).isTrue();
    assertThat(registry.size()).isZero();
    assertThat(registry.getAllResults()).isEmpty();
    assertThat(registry.getAllOutcomes()).isEmpty();
  }

  // ========================================
  // Size and IsEmpty Tests
  // ========================================

  @Test
  void sizeReturnsNumberOfResults() {
    // Given
    registry.put("job1", new ImportResult(Optional.of("user1")));
    registry.put("job2", new ImportResult(Optional.of("user2")));

    // When/Then
    assertThat(registry.size()).isEqualTo(2);
  }

  @Test
  void sizeReturnsZeroForEmptyRegistry() {
    // When/Then
    assertThat(registry.size()).isZero();
  }

  @Test
  void isEmptyReturnsTrueForNewRegistry() {
    // When/Then
    assertThat(registry.isEmpty()).isTrue();
  }

  @Test
  void isEmptyReturnsFalseWhenResultsExist() {
    // Given
    registry.put("job1", new ImportResult(Optional.of("user1")));

    // When/Then
    assertThat(registry.isEmpty()).isFalse();
  }

  // ========================================
  // Helper Methods
  // ========================================

  private OperationOutcome createOutcome(final String message, final IssueSeverity severity) {
    final OperationOutcome outcome = new OperationOutcome();
    outcome.addIssue()
        .setSeverity(severity)
        .setCode(IssueType.INFORMATIONAL)
        .setDiagnostics(message);
    return outcome;
  }
}
