/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.encoding;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.Quantity.QuantityComparator;

public class QuantityEncoding {

  @Nullable
  public static Row encode(@Nullable final Quantity quantity) {
    if (quantity == null) {
      return null;
    }
    return RowFactory.create(quantity.getId(), quantity.getValue(),
        quantity.getComparator().toCode(), quantity.getUnit(), quantity.getSystem(),
        quantity.getCode(), null /* _fid */);
  }

  @Nonnull
  public static Quantity decode(@Nonnull final Row row) {
    final Quantity quantity = new Quantity();
    quantity.setId(row.getString(0));
    quantity.setValue(row.getDecimal(1));
    quantity.setComparator(QuantityComparator.fromCode(row.getString(2)));
    quantity.setUnit(row.getString(3));
    quantity.setSystem(row.getString(4));
    quantity.setCode(row.getString(5));
    return quantity;
  }

}
