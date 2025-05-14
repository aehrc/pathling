library(sparklyr)
library(pathling)

pc <- pathling_connect()

data_source <- pc %>% pathling_read_ndjson(pathling_examples('ndjson'))

result <- data_source %>% ds_view(
    resource = 'Patient',
    select = list(
        list(
            column = list(
                list(path = 'id', name = 'id'),
                list(path = 'gender', name = 'gender'),
                list(path = "telecom.where(system='phone').value",
                     name = 'phone_numbers', collection = TRUE)
            )
        ),
        list(
            forEach = 'name',
            column = list(
                list(path = 'use', name = 'name_use'),
                list(path = 'family', name = 'family_name')
            ),
            select = list(
                list(
                    forEachOrNull = 'given',
                    column = list(
                        list(path = '$this', name = 'given_name')
                    )
                )
            )
        )
    ),
    where = list(
        list(
            path = "gender = 'male'"
        )
    )
)

result %>% show()

pc %>% pathling_disconnect()
