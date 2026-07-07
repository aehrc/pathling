/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// FHIR ValueSet Compose Language (VCL) v1 grammar, transcribed verbatim from the
// FHIR IG Guidance specification: https://build.fhir.org/ig/FHIR/ig-guidance/vcl.html
grammar Vcl;

vcl         : expr EOF ;
expr        : subExpr (conjunction | disjunction | exclusion )? ;
subExpr     : systemUri? (simpleExpr | OPEN expr CLOSE);
conjunction : (COMMA subExpr)+ ;
disjunction : (SEMI subExpr)+ ;
exclusion   : DASH subExpr ;
simpleExpr  : STAR | code | filter | includeVs ;

includeVs   : IN (URI | systemUri) ;
systemUri   : OPEN URI CLOSE;
filter      : (property
                ( EQ code
                | IS_A code
                | IS_NOT_A code
                | DESC_OF code
                | REGEX str
                | IN (codeList | URI | filterList)
                | NOT_IN (codeList | URI | filterList)
                | GENERALIZES code
                | CHILD_OF code
                | DESC_LEAF code
                | EXISTS code
                )
              | (code | codeList | STAR | URI | filterList ) DOT property
              );
filterList  : LCRLY filter (COMMA filter)* RCRLY ;
property    : code ;

codeList    : LCRLY code (COMMA code)+ RCRLY ;
code        : SCODE | QUOTED_VALUE ;
str         : QUOTED_VALUE ;

DASH        : '-' ;
OPEN        : '(' ;
CLOSE       : ')' ;
LCRLY       : '{' ;
RCRLY       : '}' ;
SEMI        : ';' ;
COMMA       : ',' ;
DOT         : '.' ;
STAR        : '*' ;

EQ          : '=' ;
IS_A        : '<<' ;
IS_NOT_A    : '~<<' ;
DESC_OF     : '<' ;
REGEX       : '/' ;
IN          : '^' ;
NOT_IN      : '~^' ;
GENERALIZES : '>>' ;
CHILD_OF    : '<!' ;
DESC_LEAF   : '!!<' ;
EXISTS      : '?' ;

URI          : [a-zA-Z]+ [:] [a-zA-Z0-9?=:;&_%+-.@#$^!{}/]+ ('|' ~[|()] *)? ;
SCODE        : [a-zA-Z0-9] [-_a-zA-Z0-9]* ;
QUOTED_VALUE : '"' (~["\\] | '\\' ["\\])* '"' ;

WS           : [ \t]+ -> skip ;
