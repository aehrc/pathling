/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.encoding;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.GenericArrayData;

/**
 * Object decoders for {{SimpleCoding}} and collections of it.
 */
public interface SimpleCodingsDecoders {

  @Nullable
  private static String safeGetString(@Nonnull final InternalRow ir, int ordinal) {
    return ir.isNullAt(ordinal)
           ? null
           : ir.getString(ordinal);
  }

  /**
   * Decodes a simple coding from an {{InternalRow}} with struct[Coding]
   *
   * @param row the InternalRow to decode.
   * @return decoded SimpleCoding.
   */
  @Nullable
  static SimpleCoding decodeCoding(@Nullable final Object row) {
    InternalRow ir = (InternalRow) row;
    return ir != null
           ? new SimpleCoding(safeGetString(ir, 1), safeGetString(ir, 3), safeGetString(ir, 2))
           : null;
  }


  /**
   * Decodes a list of SimpleCodings from an array(struct[Coding])
   *
   * @param array the GenericArrayData to decode
   * @return decoded list of SimpleCodings
   */
  @Nullable
  static List<SimpleCoding> decodeList(@Nullable final Object array) {
    GenericArrayData arrayData = (GenericArrayData) array;
    return array != null
           ? Stream.of(arrayData.array()).map(SimpleCodingsDecoders::decodeCoding)
               .collect(Collectors.toList())
           : null;
  }


  /**
   * Decodes a pair of list of codings from a struct(array(struct[Coding]), array(struct[Coding]))
   *
   * @param row the Internal row to decode
   * @return the decoded pair of list of SimpleCodings
   */
  @Nullable
  static ImmutablePair<List<SimpleCoding>, List<SimpleCoding>> decodeListPair(
      @Nullable final Object row) {
    InternalRow ir = (InternalRow) row;
    return row != null
           ? ImmutablePair.of(decodeList(ir.getArray(0)), decodeList(ir.getArray(1)))
           : null;
  }
}
