library(sparklyr)
library(pathling)

pc <- ptl_connect()

# copy the R data fraame to the spark data frame
conditions_sdf <- pc %>% ptl_spark() %>% copy_to(conditions, overwrite = TRUE)

VIRAL_DISEASE_ECL <- '<< 64572001|Disease| : (
      << 370135005|Pathological process| = << 441862004|Infectious process|,
      << 246075003|Causative agent| = << 49872002|Virus|
    )'


result <- conditions_sdf %>% mutate(
  CODE,
  IS_VIRAL_DISEASE = !!trm_member_of(!!trm_to_snomed_coding(CODE), !!trm_to_ecl_value_set(VIRAL_DISEASE_ECL)),
  .keep="none"
)  %>% collect()

pc %>% ptl_disconnect()

# as we used collect() `result` is the R data frame
result %>% show()
