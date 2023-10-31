library(sparklyr)
library(pathling)

pc <- pathling_connect()

json_resources <- pathling_spark(pc) %>% spark_read_text(pathling_examples('ndjson'))

pc %>% pathling_encode(json_resources, 'Condition') %>% show()

pc %>% pathling_disconnect()


