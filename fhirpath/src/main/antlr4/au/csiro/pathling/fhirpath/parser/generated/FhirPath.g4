// Copyright 2025 Commonwealth Scientific and Industrial Research
// Organisation (CSIRO) ABN 41 687 119 230.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Based on the FHIRPath grammar file at: http://hl7.org/fhirpath/grammar.html

grammar FhirPath;

// Grammar rules
// [FHIRPath](http://hl7.org/fhirpath/N1) Normative Release

//prog: line (line)*;
//line: ID ( '(' expr ')') ':' expr '\r'? '\n';

entireExpression
        : expression EOF
        ;
        
expression
        : term                                                      #termExpression
        | expression '.' invocation                                 #invocationExpression
        | expression '[' expression ']'                             #indexerExpression
        | ('+' | '-') expression                                    #polarityExpression
        | expression ('*' | '/' | 'div' | 'mod') expression         #multiplicativeExpression
        | expression ('+' | '-' | '&') expression                   #additiveExpression
        | expression ('is' | 'as') typeSpecifier                    #typeExpression
        | expression '|' expression                                 #unionExpression
        | expression ('<=' | '<' | '>' | '>=') expression           #inequalityExpression
        | expression ('=' | '~' | '!=' | '!~') expression           #equalityExpression
        | expression ('in' | 'contains') expression                 #membershipExpression
        | expression 'and' expression                               #andExpression
        | expression ('or' | 'xor') expression                      #orExpression
        | expression 'implies' expression                           #impliesExpression
        //| (IDENTIFIER)? '=>' expression                             #lambdaExpression
        ;

term
        : invocation                                            #invocationTerm
        | literal                                               #literalTerm
        | externalConstant                                      #externalConstantTerm
        | '(' expression ')'                                    #parenthesizedTerm
        ;

literal
        : '{' '}'                                               #nullLiteral
        | ('true' | 'false')                                    #booleanLiteral
        | STRING                                                #stringLiteral
        | NUMBER                                                #numberLiteral
        | LONGNUMBER                                            #longNumberLiteral
        | DATE                                                  #dateLiteral
        | DATETIME                                              #dateTimeLiteral
        | TIME                                                  #timeLiteral
        | quantity                                              #quantityLiteral
        | CODING                                                #codingLiteral
        ;

externalConstant
        : '%' ( identifier | STRING )
        ;

invocation                          // Terms that can be used after the function/member invocation '.'
        : identifier                                            #memberInvocation
        | function                                              #functionInvocation
        | '$this'                                               #thisInvocation
        | '$index'                                              #indexInvocation
        | '$total'                                              #totalInvocation
        ;

function
        : identifier '(' paramList? ')'
        ;

paramList
        : expression (',' expression)*
        ;

quantity
        : NUMBER unit?
        ;

unit
        : dateTimePrecision
        | pluralDateTimePrecision
        | STRING // UCUM syntax for units of measure
        ;

dateTimePrecision
        : 'year' | 'month' | 'week' | 'day' | 'hour' | 'minute' | 'second' | 'millisecond'
        ;

pluralDateTimePrecision
        : 'years' | 'months' | 'weeks' | 'days' | 'hours' | 'minutes' | 'seconds' | 'milliseconds'
        ;

typeSpecifier
        : qualifiedIdentifier
        ;

qualifiedIdentifier
        : identifier ('.' identifier)*
        ;

identifier
        : IDENTIFIER
        | DELIMITEDIDENTIFIER
        | 'as'
        | 'contains'
        | 'in'
        | 'is'
        ;


/****************************************************************
    Lexical rules
*****************************************************************/

/*
NOTE: The goal of these rules in the grammar is to provide a date
token to the parser. As such it is not attempting to validate that
the date is a correct date, that task is for the parser or interpreter.
*/

DATE
        : '@' DATEFORMAT
        ;

DATETIME
        : '@' DATEFORMAT 'T' (TIMEFORMAT TIMEZONEOFFSETFORMAT?)?
        ;

TIME
        : '@' 'T' TIMEFORMAT
        ;

fragment DATEFORMAT
        : [0-9][0-9][0-9][0-9] ('-'[0-9][0-9] ('-'[0-9][0-9])?)?
        ;

fragment TIMEFORMAT
        : [0-9][0-9] (':'[0-9][0-9] (':'[0-9][0-9] ('.'[0-9]+)?)?)?
        ;

fragment TIMEZONEOFFSETFORMAT
        : ('Z' | ('+' | '-') [0-9][0-9]':'[0-9][0-9])
        ;

IDENTIFIER
        : ([A-Za-z] | '_')([A-Za-z0-9] | '_')*            // Added _ to support CQL (FHIR could constrain it out)
        ;

DELIMITEDIDENTIFIER
        : '`' (ESC | .)*? '`'
        ;

STRING
        : '\'' (ESC | .)*? '\''
        ;
        
CODING
        : CODING_COMPONENT '|' CODING_COMPONENT ( '|' CODING_COMPONENT )? ( '|' CODING_COMPONENT )? ( '|' CODING_COMPONENT )?
        ;

fragment CODING_COMPONENT
        : RAW_CODING_COMPONENT
        | QUOTED_CODING_COMPONENT
        ;

fragment RAW_CODING_COMPONENT
        : ~[ '|\r\n\t(),]*
        ;

fragment QUOTED_CODING_COMPONENT
        : STRING
        ;

// Also allows leading zeroes now (just like CQL and XSD)
NUMBER
        : [0-9]+('.' [0-9]+)?
        ;

LONGNUMBER
        : [0-9]+ 'L'?
        ;

// Pipe whitespace to the HIDDEN channel to support retrieving source text through the parser.
WS
        : [ \r\n\t]+ -> channel(HIDDEN)
        ;

COMMENT
        : '/*' .*? '*/' -> channel(HIDDEN)
        ;

LINE_COMMENT
        : '//' ~[\r\n]* -> channel(HIDDEN)
        ;

fragment ESC
        : '\\' ([`'\\/fnrt] | UNICODE)    // allow \`, \', \\, \/, \f, etc. and \uXXX
        ;

fragment UNICODE
        : 'u' HEX HEX HEX HEX
        ;

fragment HEX
        : [0-9a-fA-F]
        ;
