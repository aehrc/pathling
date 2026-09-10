# VCL formal grammar

Reference copy of the ANTLR grammar from
https://build.fhir.org/ig/FHIR/ig-guidance/vcl.html.

Load this when you need to verify a precise syntactic question (e.g. what
characters are valid in an unquoted code, where round brackets are required, or
which operators accept which right-hand side).

```antlr
grammar VCL;

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
```

## Lexical notes

- `WS` (spaces and tabs) is skipped. Newlines and carriage returns are not
  permitted anywhere in a VCL expression.
- `SCODE` (unquoted code) starts with an alphanumeric and may contain letters,
  digits, hyphens and underscores only. Anything else (period, slash, colon,
  whitespace, etc.) requires a `QUOTED_VALUE`.
- `URI` requires an ASCII-letter scheme, e.g. `http:`, `urn:`. The grammar
  excludes round brackets from URIs because `(` and `)` delimit `systemUri`.
  Percent-encode `(` as `%28` and `)` as `%29` inside a URI.
- `QUOTED_VALUE` uses double quotes. Only `"` and `\` are escapable, both with
  a leading backslash (`\"` and `\\`).
- A URI may carry a version suffix using `|`, e.g. `http://loinc.org|2.74`.

## Operator summary

| Operator        | Symbol | Right-hand side                         |
| --------------- | ------ | --------------------------------------- |
| equals          | `=`    | code                                    |
| is-a            | `<<`   | code                                    |
| is-not-a        | `~<<`  | code                                    |
| descendent-of   | `<`    | code                                    |
| child-of        | `<!`   | code                                    |
| descendent-leaf | `!!<`  | code                                    |
| generalizes     | `>>`   | code                                    |
| regex           | `/`    | quoted string                           |
| in              | `^`    | code list, URI, or filter list          |
| not-in          | `~^`   | code list, URI, or filter list          |
| exists          | `?`    | code (the property name)                |
| of (reverse)    | `.`    | property; left-hand side is code(s)/URI |

## Set operators

- Conjunction (AND): `,`
- Disjunction (OR): `;`
- Exclusion: `-`
- Grouping: `( ... )`

There are no precedence rules between `,` and `;`. Use brackets to disambiguate
mixed expressions.
