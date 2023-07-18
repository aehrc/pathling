library(sparklyr)
library(pathlyr)

sc <- spark_connect(master = "local")
pc <- ptl_connect(sc)

NDJSON_DIR_URI <- paste0('file://', system.file('data','ndjson', package='pathlyr'))

print(sprintf('Using ndjson resources from: %s', NDJSON_DIR_URI))

data_source <- pc %>% read_ndjson(NDJSON_DIR_URI)

result <- data_source %>% ds_aggregate('Patient',
              aggregations = c(patientCount='count()', 'id.count()'),
              groupings = c('gender', givenName='name.given')
        )


