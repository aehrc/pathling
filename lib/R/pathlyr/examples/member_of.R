library(sparklyr)
library(pathlyr)

sc <- spark_connect(master = "local")
pc <- ptl_connect(sc)

data_sdf <- spark_read_csv(sc,
  system.file('data', 'csv', 'conditions.csv', package='pathlyr'), header = TRUE)

TEST_VALUE_SET <- 'http://snomed.info/sct?fhir_vs=ecl/%20%20%20%20%3C%3C%2064572001%7CDisease%7C%20%3A%20(%0A%20%20%20%20%20%20%3C%3C%20370135005%7CPathological%20process%7C%20%3D%20%3C%3C%20441862004%7CInfectious%20process%7C%2C%0A%20%20%20%20%20%20%3C%3C%20246075003%7CCausative%20agent%7C%20%3D%20%3C%3C%2049872002%7CVirus%7C%0A%20%20%20%20)'

# call the UDF directly
rs <- data_sdf %>% mutate(
  READ_CODE = member_of(!!trm_to_snomed_code(CODE), TEST_VALUE_SET))
rs %>% show()


# call the UDF via an R wrapper function (note the !!)
rs <- data_sdf %>% mutate(
  READ_CODE = !!trm_member_of(!!trm_to_snomed_code(CODE), TEST_VALUE_SET),
  XXX = 10
  ) %>% select(READ_CODE) %>% sdf_repartition(10) %>% filter(integer(READ_CODE) > 0)  %>% show()

