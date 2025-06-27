grammar AnsiSqlType;

// Parser rules
sqlType
    : characterType
    | numericType
    | booleanType
    | binaryType
    | temporalType
    | complexType
    ;

characterType
    : 'CHARACTER' ('(' length=INT ')')? 
    | 'CHAR' ('(' length=INT ')')?
    | 'CHARACTER VARYING' ('(' length=INT ')')?
    | 'VARCHAR' ('(' length=INT ')')?
    ;

numericType
    : 'NUMERIC' ('(' precision=INT (',' scale=INT)? ')')?
    | 'DECIMAL' ('(' precision=INT (',' scale=INT)? ')')?
    | 'DEC' ('(' precision=INT (',' scale=INT)? ')')?
    | 'SMALLINT'
    | 'INTEGER'
    | 'INT'
    | 'BIGINT'
    | 'FLOAT' ('(' precision=INT ')')?
    | 'REAL'
    | 'DOUBLE PRECISION'
    ;

booleanType
    : 'BOOLEAN'
    ;

binaryType
    : 'BINARY' ('(' length=INT ')')?
    | 'BINARY VARYING' ('(' length=INT ')')?
    | 'VARBINARY' ('(' length=INT ')')?
    ;

temporalType
    : 'DATE'
    | 'TIMESTAMP' ('(' precision=INT ')')? timeZone?
    | 'INTERVAL'
    ;

complexType
    : rowType
    | arrayType
    ;

rowType
    : 'ROW' ('(' fieldDefinition (',' fieldDefinition)* ')')?
    ;

arrayType
    : 'ARRAY' '<' sqlType '>'
    ;

fieldDefinition
    : fieldName=IDENTIFIER sqlType
    ;

timeZone
    : 'WITHOUT TIME ZONE'
    | 'WITH TIME ZONE'
    ;

// Lexer rules
INT : [0-9]+ ;
IDENTIFIER : [a-zA-Z_][a-zA-Z0-9_]* ;
WS : [ \t\r\n]+ -> skip ;
