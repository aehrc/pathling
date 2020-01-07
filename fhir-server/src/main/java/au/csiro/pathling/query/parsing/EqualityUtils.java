/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.parsing;

import static org.apache.spark.sql.functions.when;

import java.util.List;
import java.util.function.BiFunction;
import org.apache.spark.sql.Column;

public class EqualityUtils {

  static Column nullSafeEqual(Column left, Column right, BiFunction<Column, Column, Column> eq) {
    return when(left.isNull().and(right.isNull()), true)
        .when(left.isNull().or(right.isNull()), false).otherwise(eq.apply(left, right));
  }

  static Column nullSafeEqual(Column left, Column right) {
    return nullSafeEqual(left, right, Column::equalTo);
  }

  static Column optionalEqual(Column left, Column right) {
    return left.isNull().or(right.isNull()).or(left.equalTo(right));
  }

  static BiFunction<Column, Column, Column> structEqual(List<String> columnsToCompare) {
    return (left, right) -> columnsToCompare.stream()
        .map(f -> optionalEqual(left.getField(f), right.getField(f))).reduce(Column::and).get();
  }

}
