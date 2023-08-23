library(sparklyr)
library(pathlyr)

pc <- ptl_connect()

json_resources <- ptl_spark(pc) %>% spark_read_text(pathlyr_examples('ndjson'))

pc %>% ptl_encode(json_resources, 'Condition') %>% show()

pc %>% ptl_disconnect()


