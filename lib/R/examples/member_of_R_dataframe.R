library(sparklyr)
library(pathling)

# create a default pathling context
pc <- pathling_connect()

# copy the R data frame to the spark data frame
conditions_sdf <- pc %>%
    pathling_spark() %>%
    copy_to(conditions, overwrite = TRUE)


# define an ECL expression for viral diseases
VIRAL_DISEASE_ECL <- '<< 64572001|Disease| : (
      << 370135005|Pathological process| = << 441862004|Infectious process|,
      << 246075003|Causative agent| = << 49872002|Virus|
    )'

# use pathling terminology functions and dplr verbs to find codes for viral diseases and obtain their display names
result <- conditions_sdf %>%
    filter(!!tx_member_of(!!tx_to_snomed_coding(CODE), !!tx_to_ecl_value_set(VIRAL_DISEASE_ECL))) %>%
    mutate(DISPLAY_NAME = !!tx_display(!!tx_to_snomed_coding(CODE))) %>%
    select(CODE, DISPLAY_NAME) %>% distinct() %>%
    collect()

# disconnect from the pathling context
pc %>% pathling_disconnect()

# as we used `collect()` `result` is the R data frame
result %>% show()
