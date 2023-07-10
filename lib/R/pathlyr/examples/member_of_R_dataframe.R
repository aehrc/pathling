library(sparklyr)
library(pathlyr)


TEST_VALUE_SET='http://snomed.info/sct?fhir_vs=ecl/%20%20%20%20%3C%3C%2064572001%7CDisease%7C%20%3A%20(%0A%20%20%20%20%20%20%3C%3C%20370135005%7CPathological%20process%7C%20%3D%20%3C%3C%20441862004%7CInfectious%20process%7C%2C%0A%20%20%20%20%20%20%3C%3C%20246075003%7CCausative%20agent%7C%20%3D%20%3C%3C%2049872002%7CVirus%7C%0A%20%20%20%20)'


sc <- spark_connect(master = "local")
pc <- ptl_connect(sc)


# load data as R dataframe
data(conditions)
conditions %>% show()

# call the UDF via an R wrapper function (note the !!)
result_df <- sc %>%
  copy_to(conditions,  overwrite = TRUE) %>%
  mutate(READ_CODE = !!trm_member_of(!!snomed_code(CODE), TEST_VALUE_SET)) %>%
  collect()

result_df %>% show()
