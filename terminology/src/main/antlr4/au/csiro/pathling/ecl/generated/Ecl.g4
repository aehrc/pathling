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

// A SNOMED CT Expression Constraint Language (ECL v2) recognition grammar. It parses the whole
// construct space Pathling can recognise - including role groups, cardinality, reverse flags,
// concrete values, filters and history supplements - so that the translator, not the parser,
// rejects unsupported constructs with an error that names them.
grammar Ecl;

expressionConstraint : expr EOF ;

expr      : orExpr ;
orExpr    : andExpr (OR andExpr)* ;
andExpr   : minusExpr (AND minusExpr)* ;
minusExpr : refinedExpr (MINUS refinedExpr)* ;

refinedExpr : subExpr (COLON refinement)? braceConstraint* ;

braceConstraint : BRACE_CONSTRAINT ;

subExpr : constraint ;

constraint : dottedExpr | boundExpr ;

dottedExpr : boundExpr (DOT eclAttributeName)+ ;

boundExpr : constraintOperator? focus ;

focus : memberOf | eclConceptReference | wildCard | LPAREN expr RPAREN ;

memberOf : MEMBER (eclConceptReference | wildCard | LPAREN expr RPAREN) ;

eclConceptReference : SCTID PIPE_TERM? ;

wildCard : WILDCARD ;

constraintOperator
    : CHILD_OR_SELF
    | DESC_OR_SELF
    | CHILD
    | DESC
    | PARENT_OR_SELF
    | ANC_OR_SELF
    | PARENT
    | ANC
    ;

refinement : subRefinement ((COMMA | AND | OR) subRefinement)* ;

subRefinement : eclAttributeGroup | eclAttributeSet | LPAREN refinement RPAREN ;

eclAttributeGroup : cardinality? LBRACE eclAttributeSet RBRACE ;

eclAttributeSet : subAttribute ((COMMA | AND | OR) subAttribute)* ;

subAttribute : eclAttribute | LPAREN eclAttributeSet RPAREN ;

eclAttribute : cardinality? REVERSE? eclAttributeName comparison attributeValue ;

eclAttributeName : constraintOperator? eclConceptReference | wildCard ;

comparison : EQUALS | NOTEQUALS ;

attributeValue : concreteValue | subExpr | LPAREN expr RPAREN ;

concreteValue : HASH SCTID | STRING ;

cardinality : LBRACKET SCTID DOTDOT (SCTID | WILDCARD) RBRACKET ;

// Filters and history supplements are captured whole and rejected at translation.
BRACE_CONSTRAINT : '{{' .*? '}}' ;

CHILD_OR_SELF  : '<<!' ;
PARENT_OR_SELF : '>>!' ;
DESC_OR_SELF   : '<<' ;
ANC_OR_SELF    : '>>' ;
CHILD          : '<!' ;
PARENT         : '>!' ;
DESC           : '<' ;
ANC            : '>' ;
MEMBER         : '^' ;
WILDCARD       : '*' ;
NOTEQUALS      : '!=' ;
EQUALS         : '=' ;
COLON          : ':' ;
COMMA          : ',' ;
DOTDOT         : '..' ;
DOT            : '.' ;
LPAREN         : '(' ;
RPAREN         : ')' ;
LBRACE         : '{' ;
RBRACE         : '}' ;
LBRACKET       : '[' ;
RBRACKET       : ']' ;
HASH           : '#' ;

AND     : [Aa][Nn][Dd] ;
OR      : [Oo][Rr] ;
MINUS   : [Mm][Ii][Nn][Uu][Ss] ;
REVERSE : [Rr] ;

SCTID     : [0-9]+ ;
PIPE_TERM : '|' ~[|]* '|' ;
STRING    : '"' (~["\\] | '\\' .)* '"' ;

WS : [ \t\r\n]+ -> skip ;
