library(sparklyr)
library(pathling)

pc <- pathling_connect()


VIRAL_DISEASE_ECL <- '<< 64572001|Disease| : (
      << 370135005|Pathological process| = << 441862004|Infectious process|,
      << 246075003|Causative agent| = << 49872002|Virus|
    )'


pc %>% pathling_example_resource('Condition') %>% mutate(
    CONDITION_ID = id,
    IS_VIRAL_DISEASE = !!tx_member_of(code[['coding']], !!tx_to_ecl_value_set(VIRAL_DISEASE_ECL)),
    .keep="none"
  )  %>% show()

pc %>% pathling_disconnect()



