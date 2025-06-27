grammar AnsiSqlType;

// Parser rules
sqlType
    : CHARACTER_TYPE
    | NUMERIC_TYPE
    | BOOLEAN_TYPE
    | BINARY_TYPE
    | TEMPORAL_TYPE
    | COMPLEX_TYPE
    ;

CHARACTER_TYPE
    : 'CHARACTER' ('(' INT ')')? 
    | 'CHAR' ('(' INT ')')?
    | 'CHARACTER VARYING' ('(' INT ')')?
    | 'VARCHAR' ('(' INT ')')?
    ;

NUMERIC_TYPE
    : 'NUMERIC' ('(' INT (',' INT)? ')')?
    | 'DECIMAL' ('(' INT (',' INT)? ')')?
    | 'DEC' ('(' INT (',' INT)? ')')?
    | 'SMALLINT'
    | 'INTEGER'
    | 'INT'
    | 'BIGINT'
    | 'FLOAT' ('(' INT ')')?
    | 'REAL'
    | 'DOUBLE PRECISION'
    ;

BOOLEAN_TYPE
    : 'BOOLEAN'
    ;

BINARY_TYPE
    : 'BINARY' ('(' INT ')')?
    | 'BINARY VARYING' ('(' INT ')')?
    | 'VARBINARY' ('(' INT ')')?
    ;

TEMPORAL_TYPE
    : 'DATE'
    | 'TIMESTAMP' ('(' INT ')')? TIME_ZONE?
    | 'INTERVAL'
    ;

COMPLEX_TYPE
    : 'ROW'
    | 'ARRAY'
    ;

TIME_ZONE
    : 'WITHOUT TIME ZONE'
    | 'WITH TIME ZONE'
    ;

// Lexer rules
INT : [0-9]+ ;
WS : [ \t\r\n]+ -> skip ;
