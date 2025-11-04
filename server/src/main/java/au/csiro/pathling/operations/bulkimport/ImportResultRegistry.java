package au.csiro.pathling.operations.bulkimport;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for storing import job results and associated operation outcomes.
 *
 * @author Felix Naumann
 */
@Slf4j
@Component
@Profile("core")
public class ImportResultRegistry {

  private final ConcurrentHashMap<String, ImportResult> results = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, List<OperationOutcome>> outcomes = new ConcurrentHashMap<>();

  /**
   * Associates an import result with the specified job ID.
   *
   * @param jobId the job identifier
   * @param result the import result
   * @return this registry for method chaining
   */
  @Nonnull
  public ImportResultRegistry put(@Nonnull final String jobId, @Nonnull final ImportResult result) {
    results.put(jobId, result);
    return this;
  }

  /**
   * Associates an import result with the specified job ID.
   * Functional-style variant that returns the result for use in streams.
   *
   * @param jobId the job identifier
   * @param result the import result
   * @return the import result that was stored
   */
  @Nonnull
  public ImportResult putAndReturn(@Nonnull final String jobId, @Nonnull final ImportResult result) {
    results.put(jobId, result);
    return result;
  }

  /**
   * Retrieves the import result for the specified job ID.
   *
   * @param jobId the job identifier
   * @return an Optional containing the result if present, empty otherwise
   */
  @Nonnull
  public Optional<ImportResult> get(@Nonnull final String jobId) {
    return Optional.ofNullable(results.get(jobId));
  }

  /**
   * Adds an operation outcome to the list associated with the specified job ID.
   *
   * @param jobId the job identifier
   * @param outcome the operation outcome to add
   * @return this registry for method chaining
   */
  @Nonnull
  public ImportResultRegistry addOutcome(@Nonnull final String jobId,
      @Nonnull final OperationOutcome outcome) {
    outcomes.computeIfAbsent(jobId, k -> new ArrayList<>()).add(outcome);
    return this;
  }

  /**
   * Adds an operation outcome to the list associated with the specified job ID.
   * Functional-style variant that returns the outcome for use in streams.
   *
   * @param jobId the job identifier
   * @param outcome the operation outcome to add
   * @return the operation outcome that was added
   */
  @Nonnull
  public OperationOutcome addOutcomeAndReturn(@Nonnull final String jobId,
      @Nonnull final OperationOutcome outcome) {
    outcomes.computeIfAbsent(jobId, k -> new ArrayList<>()).add(outcome);
    return outcome;
  }

  /**
   * Adds multiple operation outcomes to the list associated with the specified job ID.
   *
   * @param jobId the job identifier
   * @param outcomesToAdd the collection of operation outcomes to add
   * @return this registry for method chaining
   */
  @Nonnull
  public ImportResultRegistry addOutcomes(@Nonnull final String jobId,
      @Nonnull final Collection<OperationOutcome> outcomesToAdd) {
    outcomes.computeIfAbsent(jobId, k -> new ArrayList<>()).addAll(outcomesToAdd);
    return this;
  }

  /**
   * Retrieves an immutable copy of the operation outcomes for the specified job ID.
   *
   * @param jobId the job identifier
   * @return an immutable list of operation outcomes, empty if none exist
   */
  @Nonnull
  public List<OperationOutcome> getOutcomes(@Nonnull final String jobId) {
    final List<OperationOutcome> list = outcomes.get(jobId);
    return list != null ? Collections.unmodifiableList(new ArrayList<>(list)) : List.of();
  }

  /**
   * Retrieves an immutable copy of all import results.
   *
   * @return an immutable map of job IDs to import results
   */
  @Nonnull
  public Map<String, ImportResult> getAllResults() {
    return Collections.unmodifiableMap(new ConcurrentHashMap<>(results));
  }

  /**
   * Retrieves an immutable copy of all operation outcomes.
   *
   * @return an immutable map of job IDs to lists of operation outcomes
   */
  @Nonnull
  public Map<String, List<OperationOutcome>> getAllOutcomes() {
    final Map<String, List<OperationOutcome>> copy = new ConcurrentHashMap<>();
    outcomes.forEach((key, value) ->
        copy.put(key, Collections.unmodifiableList(new ArrayList<>(value))));
    return Collections.unmodifiableMap(copy);
  }

  /**
   * Removes the import result and all associated outcomes for the specified job ID.
   *
   * @param jobId the job identifier
   * @return the removed import result, or null if not found
   */
  @Nullable
  public ImportResult remove(@Nonnull final String jobId) {
    outcomes.remove(jobId);
    return results.remove(jobId);
  }

  /**
   * Checks if the registry contains an import result for the specified job ID.
   *
   * @param jobId the job identifier
   * @return true if a result exists, false otherwise
   */
  public boolean containsKey(@Nonnull final String jobId) {
    return results.containsKey(jobId);
  }

  /**
   * Removes all entries from the registry.
   */
  public void clear() {
    results.clear();
    outcomes.clear();
  }

  /**
   * Returns the number of import results in the registry.
   *
   * @return the number of entries
   */
  public int size() {
    return results.size();
  }

  /**
   * Checks if the registry is empty.
   *
   * @return true if the registry contains no entries, false otherwise
   */
  public boolean isEmpty() {
    return results.isEmpty();
  }
}
