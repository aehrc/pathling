library(sparklyr)
library(pathlyr)

pc <- ptl_connect()

data_source <- pc %>% ptl_read_ndjson(pathlyr_examples('ndjson'))

result <- data_source %>% ds_aggregate('Patient',
              aggregations = c(patientCount='count()', 'id.count()'),
              groupings = c('gender', givenName='name.given')
        )

result %>% show()

pc %>% ptl_disconnect()
