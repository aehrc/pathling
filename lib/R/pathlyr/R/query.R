#' @import sparklyr

for_each_with_name <-function(sequence, FUN, ...) {
  sequence_names <- names(sequence)
  for (i in seq_along(sequence)) {
    name <- if (is.null(sequence_names)) "" else sequence_names[i]
    value <- sequence[i]
    FUN(value, name, ...)
  }
}

#'@export
aggregate <- function(ds, subject_resource, aggregations, groupings = c()) {
  q <-j_invoke(ds, "aggregate", as.character(subject_resource))
  
  for_each_with_name(aggregations, function(expression, name) {
    if (nchar(name) == 0) {
      q <-j_invoke(q, "withAggregation", expression)
    } else {
      q <-j_invoke(q, "withAggregation", expression, name)
    }
  })    
  
  for_each_with_name(groupings, function(expression, name) {
    if (nchar(name) == 0) {
      q <-j_invoke(q, "withGrouping", expression)
    } else {
      q <-j_invoke(q, "withGrouping", expression, name)
    }
  })      
  sdf_register(j_invoke(q, "execute"))
}
