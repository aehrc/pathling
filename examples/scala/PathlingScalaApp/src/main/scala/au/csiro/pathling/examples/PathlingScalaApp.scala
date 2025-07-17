package au.csiro.pathling.examples

import au.csiro.pathling.library.PathlingContext
import au.csiro.pathling.views.FhirView
import au.csiro.pathling.views.FhirView.{column, columns, forEach}

object PathlingScalaApp {

  def main(args: Array[String]): Unit = {
    val pc = PathlingContext.create()
    val data = pc.read().ndjson("/tmp/ndjson")

    val view = FhirView.ofResource("Observation")
      .select(
        columns(
          column("patient_id", "getResourceKey()")
        ),
        forEach("code.coding",
          column("code_system", "system"),
          column("code_code", "code"),
          column("code_display", "display")
        )
      ).build()

    val result = data.view(view).execute()

    result.show(truncate = false)
  }
}
