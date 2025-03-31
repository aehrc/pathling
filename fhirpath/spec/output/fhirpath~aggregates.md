## Aggregates

> **Note:** The contents of this section are Standard for Trial Use
> (STU)

FHIRPath supports a general-purpose aggregate function to enable the
calculation of aggregates such as sum, min, and max to be expressed:

### aggregate(aggregator : expression \[, init : value\]) : value 

Performs general-purpose aggregation by evaluating the aggregator
expression for each element of the input collection. Within this
expression, the standard iteration variables of
`$this` and
`$index` can be accessed, but
also a `$total` aggregation
variable.

The value of the `$total`
variable is set to `init`, or
empty (`{ }`) if no
`init` value is supplied, and is
set to the result of the aggregator expression after every iteration.\
The result of the aggregate function is the value of
`$total` after the last
iteration.

Using this function, sum can be expressed as:

``` stu
value.aggregate($this + $total, 0)
```

Min can be expressed as:

``` stu
value.aggregate(iif($total.empty(), $this, iif($this < $total, $this, $total)))
```

and average would be expressed as:

``` stu
value.aggregate($total + $this, 0) / value.count()
```