library(sparklyr)
library(pathlyr)


sc <- spark_connect(master = "local")

# Note that the enable_extensions parameter is set to FALSE, which produces schema that is compatible
# with sparkly which does not support map fields with integer keys required for representing extensions.
pc <- ptl_connect(sc, enable_extensions=FALSE)

json_resources <- spark_read_text(sc, path=system.file('data','ndjson', package='pathlyr'))
pc %>% ptl_encode(json_resources, 'Condition') %>% show()


