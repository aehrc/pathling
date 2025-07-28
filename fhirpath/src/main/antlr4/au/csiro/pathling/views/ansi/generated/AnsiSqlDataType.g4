grammar AnsiSqlDataType;

// Define case-insensitive letters
fragment A: [Aa];
fragment B: [Bb];
fragment C: [Cc];
fragment D: [Dd];
fragment E: [Ee];
fragment F: [Ff];
fragment G: [Gg];
fragment H: [Hh];
fragment I: [Ii];
fragment J: [Jj];
fragment K: [Kk];
fragment L: [Ll];
fragment M: [Mm];
fragment N: [Nn];
fragment O: [Oo];
fragment P: [Pp];
fragment Q: [Qq];
fragment R: [Rr];
fragment S: [Ss];
fragment T: [Tt];
fragment U: [Uu];
fragment V: [Vv];
fragment W: [Ww];
fragment X: [Xx];
fragment Y: [Yy];
fragment Z: [Zz];


// Keywords
K_CHARACTER: C H A R A C T E R;
K_CHAR: C H A R;
K_VARYING: V A R Y I N G;
K_VARCHAR: V A R C H A R;
K_NUMERIC: N U M E R I C;
K_DECIMAL: D E C I M A L;
K_DEC: D E C;
K_SMALLINT: S M A L L I N T;
K_INTEGER: I N T E G E R;
K_INT: I N T;
K_BIGINT: B I G I N T;
K_FLOAT: F L O A T;
K_REAL: R E A L;
K_DOUBLE: D O U B L E;
K_PRECISION: P R E C I S I O N;
K_BOOLEAN: B O O L E A N;
K_BINARY: B I N A R Y;
K_VARBINARY: V A R B I N A R Y;
K_DATE: D A T E;
K_TIMESTAMP: T I M E S T A M P;
K_INTERVAL: I N T E R V A L;
K_ROW: R O W;
K_ARRAY: A R R A Y;
K_WITHOUT: W I T H O U T;
K_WITH: W I T H;
K_TIME: T I M E;
K_ZONE: Z O N E;

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
    : K_CHARACTER ('(' length=INT ')')? 
    | K_CHAR ('(' length=INT ')')?
    | K_CHARACTER K_VARYING ('(' length=INT ')')?
    | K_VARCHAR ('(' length=INT ')')?
    ;

numericType
    : K_NUMERIC ('(' precision=INT (',' scale=INT)? ')')?
    | K_DECIMAL ('(' precision=INT (',' scale=INT)? ')')?
    | K_DEC ('(' precision=INT (',' scale=INT)? ')')?
    | K_SMALLINT
    | K_INTEGER
    | K_INT
    | K_BIGINT
    | K_FLOAT ('(' precision=INT ')')?
    | K_REAL
    | K_DOUBLE K_PRECISION
    ;

booleanType
    : K_BOOLEAN
    ;

binaryType
    : K_BINARY ('(' length=INT ')')?
    | K_BINARY K_VARYING ('(' length=INT ')')?
    | K_VARBINARY ('(' length=INT ')')?
    ;

temporalType
    : K_DATE
    | K_TIMESTAMP ('(' precision=INT ')')? timeZone?
    | K_INTERVAL
    ;

complexType
    : rowType
    | arrayType
    ;

rowType
    : K_ROW ('(' fieldDefinition (',' fieldDefinition)* ')')?
    ;

arrayType
    : K_ARRAY '<' sqlType '>'
    ;

fieldDefinition
    : fieldName=IDENTIFIER sqlType
    ;

timeZone
    : K_WITHOUT K_TIME K_ZONE
    | K_WITH K_TIME K_ZONE
    ;

// Lexer rules
INT : [0-9]+ ;
IDENTIFIER : [a-zA-Z_][a-zA-Z0-9_]* ;
WS : [ \t\r\n]+ -> skip ;
