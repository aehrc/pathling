library(sparklyr)
library(pathling)

# create a default pathling context
pc <- ptl_connect()

# copy the R data frame to the spark data frame
conditions_sdf <- pc %>%
    ptl_spark() %>%
    copy_to(conditions, overwrite = TRUE)


# define an ECL expression for viral diseases
VIRAL_DISEASE_ECL <- '<< 64572001|Disease| : (
      << 370135005|Pathological process| = << 441862004|Infectious process|,
      << 246075003|Causative agent| = << 49872002|Virus|
    )'

# use pathling terminology functions and dplr verbs to find codes for viral diseases and obtain their display names
result <- conditions_sdf %>%
    filter(!!trm_member_of(!!trm_to_snomed_coding(CODE), !!trm_to_ecl_value_set(VIRAL_DISEASE_ECL))) %>%
    mutate(DISPLAY_NAME = !!trm_display(!!trm_to_snomed_coding(CODE))) %>%
    select(CODE, DISPLAY_NAME) %>% distinct() %>%
    collect()

# disconnect from the pathling context
pc %>% ptl_disconnect()

# as we used `collect()` `result` is the R data frame
result %>% show()
