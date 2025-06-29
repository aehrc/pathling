package au.csiro.pathling.benchmark;

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.Row;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Warmup;

@Fork(2)
@Warmup(iterations = 1, time = 5)
@Measurement(iterations = 3, time = 5)
@BenchmarkMode({Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class PathlingBenchmark {

  @Benchmark
  public List<Row> conditionFlat(@Nonnull final PathlingBenchmarkState state) {
    return state.getNdjsonSource()
        .view("Condition")
        .json(state.getViewDefinitions().get("ConditionFlat"))
        .execute()
        .collectAsList();
  }

  @Benchmark
  public List<Row> encounterFlat(@Nonnull final PathlingBenchmarkState state) {
    return state.getNdjsonSource()
        .view("Encounter")
        .json(state.getViewDefinitions().get("EncounterFlat"))
        .execute()
        .collectAsList();
  }

  @Benchmark
  public List<Row> patientAddresses(@Nonnull final PathlingBenchmarkState state) {
    return state.getNdjsonSource()
        .view("Patient")
        .json(state.getViewDefinitions().get("PatientAddresses"))
        .execute()
        .collectAsList();
  }

  @Benchmark
  public List<Row> patientAndContactAddressUnion(@Nonnull final PathlingBenchmarkState state) {
    return state.getNdjsonSource()
        .view("Patient")
        .json(state.getViewDefinitions().get("PatientAndContactAddressUnion"))
        .execute()
        .collectAsList();
  }

  @Benchmark
  public List<Row> patientDemographics(@Nonnull final PathlingBenchmarkState state) {
    return state.getNdjsonSource()
        .view("Patient")
        .json(state.getViewDefinitions().get("PatientDemographics"))
        .execute()
        .collectAsList();
  }

  @Benchmark
  public List<Row> usCoreBloodPressures(@Nonnull final PathlingBenchmarkState state) {
    return state.getNdjsonSource()
        .view("Observation")
        .json(state.getViewDefinitions().get("UsCoreBloodPressures"))
        .execute()
        .collectAsList();
  }

}
