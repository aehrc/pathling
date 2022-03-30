/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
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
 * Object decoders for {@link SimpleCoding} and collections of it.
 */
public interface SimpleCodingsDecoders {

  @Nullable
  private static String safeGetString(@Nonnull final InternalRow ir, final int ordinal) {
    return ir.isNullAt(ordinal)
           ? null
           : ir.getString(ordinal);
  }

  /**
   * Decodes a simple coding from an {@link InternalRow} with struct[Coding]
   *
   * @param row the InternalRow to decode
   * @return decoded {@link SimpleCoding}
   */
  @Nullable
  static SimpleCoding decodeCoding(@Nullable final Object row) {
    final InternalRow ir = (InternalRow) row;
    if (ir != null) {
      return new SimpleCoding(safeGetString(ir, CodingEncoding.SYSTEM_INDEX),
          safeGetString(ir, CodingEncoding.CODE_INDEX),
          safeGetString(ir, CodingEncoding.VERSION_INDEX));
    } else {
      return null;
    }
  }


  /**
   * Decodes a list of SimpleCodings from an {@code array(struct[Coding])}.
   *
   * @param array the {@link GenericArrayData} to decode
   * @return decoded list of {@link SimpleCoding}
   */
  @Nullable
  static List<SimpleCoding> decodeList(@Nullable final Object array) {
    final GenericArrayData arrayData = (GenericArrayData) array;
    return array != null
           ? Stream.of(arrayData.array()).map(SimpleCodingsDecoders::decodeCoding)
               .collect(Collectors.toList())
           : null;
  }


  /**
   * Decodes a pair of {@link List} of codings from a {@code struct(array(struct[Coding]),
   * array(struct[Coding]))}
   *
   * @param row the {@link InternalRow} to decode
   * @return the decoded pair of list of {@link SimpleCoding}
   */
  @Nullable
  static ImmutablePair<List<SimpleCoding>, List<SimpleCoding>> decodeListPair(
      @Nullable final Object row) {
    @SuppressWarnings("TypeMayBeWeakened") final InternalRow ir = (InternalRow) row;
    return row != null
           ? ImmutablePair.of(decodeList(ir.getArray(0)), decodeList(ir.getArray(1)))
           : null;
  }
}
