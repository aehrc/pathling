/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © 2018-2020, Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230. Licensed
 * under the CSIRO Open Source Software Licence Agreement.
 */

package au.csiro.pathling.encoders;

import au.csiro.pathling.encoders.codes.broadcast.BroadcastableValueSets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.IndexedSeq;

/**
 * Support for user-defined functions that use valuesets. This class offers users a stack to push
 * and pop valuesets used in the in_valuesets UDF, allowing callers to temporarily override
 * valuesets and then simply pop them to revert to the previous state.
 *
 * <p>
 * The {@code in_valueset} UDF allows the user to check whether a given CodeableConcept (or sequence
 * of CodeableConcepts) have a Coding field in a ValueSet specified by a Reference name. It can be
 * called SparkSQL as {@code in_valueset(codeProperty, 'Reference')}.
 * </p>
 */
public class ValueSetUdfs {

  /**
   * Returns true if the given CodeableConcept row has a Coding belonging to the ValueSet having the
   * given reference name, or false otherwise.
   */
  private static Boolean inValueSet(Row codeableRow,
      String referenceName,
      BroadcastableValueSets valueSets) {

    boolean found = false;

    if (codeableRow != null) {

      List<Row> codingArray = codeableRow.getList(1);

      if (codingArray != null) {

        for (Row coding : codingArray) {

          String system = coding.getAs("system");
          String code = coding.getAs("code");

          // If there exists a matching code, return true.
          if (valueSets.hasCode(referenceName, system, code)) {

            found = true;

            break;
          }
        }
      }
    }

    return found;
  }

  /**
   * Returns true if the given input CodeableConcept, or sequence of CodeableConcept, has a Coding
   * contained in the ValueSet having the given reference name, or false otherwise. This method is
   * dynamically typed as it may be invoked over either a structure or sequence of structures in
   * SparkSQL.
   */
  private static Boolean inValueSet(Object input,
      String referenceName,
      Broadcast<BroadcastableValueSets> valueSets) {

    // A null code never matches.
    if (input == null) {

      return false;
    }

    if (input instanceof Row) {

      return inValueSet((Row) input, referenceName, valueSets.getValue());
    } else {

      IndexedSeq<Row> codeableConceptSeq = (IndexedSeq<Row>) input;

      boolean found = false;

      for (int i = 0; i < codeableConceptSeq.size(); i++) {

        if (inValueSet(codeableConceptSeq.apply(i), referenceName, valueSets.getValue())) {

          found = true;

          break;
        }
      }

      return found;
    }
  }

  /**
   * Spark UDF to check FHIR resources' code-property fields for membership in a valueset. The input
   * code-field can either be a CodeableConcept or an array of CodeableConcept structures.
   */
  static class InValuesetUdf implements UDF2<Object, String, Boolean> {

    private Broadcast<BroadcastableValueSets> broadcast;

    InValuesetUdf(Broadcast<BroadcastableValueSets> broadcast) {
      this.broadcast = broadcast;
    }

    @Override
    public Boolean call(Object input, String referenceName) throws Exception {

      return inValueSet(input, referenceName, broadcast);
    }
  }

  /**
   * Stack of valuesets used in user-defined functions.
   */
  private static final Deque<Broadcast<BroadcastableValueSets>> valueSetStack = new ArrayDeque<>();

  /**
   * Pushes an "in_valueset" UDF that uses the given {@link BroadcastableValueSets} for its
   * content.
   *
   * @param spark the spark session
   * @param valueSets the valuesets to use in the UDF
   */
  public static synchronized void pushUdf(SparkSession spark, BroadcastableValueSets valueSets) {

    JavaSparkContext ctx = new JavaSparkContext(spark.sparkContext());

    Broadcast<BroadcastableValueSets> broadcast = ctx.broadcast(valueSets);

    pushUdf(spark, broadcast);
  }

  /**
   * Pushes an "in_valueset" UDF that uses an already broadcast instance of {@link
   * BroadcastableValueSets} for its content.
   *
   * @param spark the spark session
   * @param broadcast the broadcast valuesets to use in the UDF
   */
  public static synchronized void pushUdf(SparkSession spark,
      Broadcast<BroadcastableValueSets> broadcast) {

    spark.udf()
        .register("in_valueset",
            new InValuesetUdf(broadcast),
            DataTypes.BooleanType);

    // Push the broadcast variable
    valueSetStack.push(broadcast);
  }

  /**
   * Pops a BroadcastableValueSets from the user-defined function stack.
   *
   * @param spark the spark session
   * @return true if there is still a registered in_valuset UDF, false otherwise
   */
  public static synchronized boolean popUdf(SparkSession spark) {

    if (valueSetStack.isEmpty()) {

      return false;

    } else {

      // Cleanup the previously broadcast valuesets
      Broadcast<BroadcastableValueSets> old = valueSetStack.pop();

      old.destroy();

      if (valueSetStack.isEmpty()) {

        return false;

      } else {

        // Re-apply the previous function.
        Broadcast<BroadcastableValueSets> current = valueSetStack.peek();

        spark.udf()
            .register("in_valueset",
                new InValuesetUdf(current),
                DataTypes.BooleanType);

        return true;
      }
    }
  }

  /**
   * Returns the broadcast variable of the current valuesets so they can be used in other
   * user-defined functions.
   *
   * @return the broadcast variable of the current valuesets, or null if none exists.
   */
  public static synchronized Broadcast<BroadcastableValueSets> currentValueSets() {

    return valueSetStack.isEmpty()
        ? null
        : valueSetStack.peek();
  }
}
