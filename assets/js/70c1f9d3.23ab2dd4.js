"use strict";(self.webpackChunkpathling_site=self.webpackChunkpathling_site||[]).push([[311],{3905:(e,t,n)=>{n.d(t,{Zo:()=>m,kt:()=>c});var a=n(7294);function i(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function r(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function l(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?r(Object(n),!0).forEach((function(t){i(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):r(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function o(e,t){if(null==e)return{};var n,a,i=function(e,t){if(null==e)return{};var n,a,i={},r=Object.keys(e);for(a=0;a<r.length;a++)n=r[a],t.indexOf(n)>=0||(i[n]=e[n]);return i}(e,t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);for(a=0;a<r.length;a++)n=r[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(i[n]=e[n])}return i}var p=a.createContext({}),s=function(e){var t=a.useContext(p),n=t;return e&&(n="function"==typeof e?e(t):l(l({},t),e)),n},m=function(e){var t=s(e.components);return a.createElement(p.Provider,{value:t},e.children)},d="mdxType",u={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},h=a.forwardRef((function(e,t){var n=e.components,i=e.mdxType,r=e.originalType,p=e.parentName,m=o(e,["components","mdxType","originalType","parentName"]),d=s(n),h=i,c=d["".concat(p,".").concat(h)]||d[h]||u[h]||r;return n?a.createElement(c,l(l({ref:t},m),{},{components:n})):a.createElement(c,l({ref:t},m))}));function c(e,t){var n=arguments,i=t&&t.mdxType;if("string"==typeof e||i){var r=n.length,l=new Array(r);l[0]=h;var o={};for(var p in t)hasOwnProperty.call(t,p)&&(o[p]=t[p]);o.originalType=e,o[d]="string"==typeof e?e:i,l[1]=o;for(var s=2;s<r;s++)l[s]=n[s];return a.createElement.apply(null,l)}return a.createElement.apply(null,n)}h.displayName="MDXCreateElement"},2633:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>p,contentTitle:()=>l,default:()=>u,frontMatter:()=>r,metadata:()=>o,toc:()=>s});var a=n(7462),i=(n(7294),n(3905));const r={sidebar_position:2},l="Data types",o={unversionedId:"fhirpath/data-types",id:"fhirpath/data-types",title:"Data types",description:"The FHIRPath implementation within Pathling supports the following types of",source:"@site/docs/fhirpath/data-types.md",sourceDirName:"fhirpath",slug:"/fhirpath/data-types",permalink:"/docs/fhirpath/data-types",draft:!1,editUrl:"https://github.com/aehrc/pathling/tree/main/site/docs/fhirpath/data-types.md",tags:[],version:"current",sidebarPosition:2,frontMatter:{sidebar_position:2},sidebar:"fhirpath",previous:{title:"Introduction",permalink:"/docs/fhirpath/"},next:{title:"Operators",permalink:"/docs/fhirpath/operators"}},p={},s=[{value:"Boolean",id:"boolean",level:2},{value:"String",id:"string",level:2},{value:"Integer",id:"integer",level:2},{value:"Decimal",id:"decimal",level:2},{value:"DateTime",id:"datetime",level:2},{value:"Date",id:"date",level:2},{value:"Time",id:"time",level:2},{value:"Quantity",id:"quantity",level:2},{value:"Coding",id:"coding",level:2},{value:"Materializable types",id:"materializable-types",level:2}],m={toc:s},d="wrapper";function u(e){let{components:t,...n}=e;return(0,i.kt)(d,(0,a.Z)({},m,n,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("h1",{id:"data-types"},"Data types"),(0,i.kt)("p",null,"The FHIRPath implementation within Pathling supports the following types of\nliteral expressions:"),(0,i.kt)("p",null,"See also: ",(0,i.kt)("a",{parentName:"p",href:"https://hl7.org/fhirpath/#literals"},"Literals")," and\n",(0,i.kt)("a",{parentName:"p",href:"https://hl7.org/fhir/R4/fhirpath.html#types"},"Using FHIR types in expressions")),(0,i.kt)("h2",{id:"boolean"},"Boolean"),(0,i.kt)("p",null,"The Boolean type represents the logical Boolean values ",(0,i.kt)("inlineCode",{parentName:"p"},"true")," and ",(0,i.kt)("inlineCode",{parentName:"p"},"false"),"."),(0,i.kt)("p",null,"Examples:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"true\nfalse\n")),(0,i.kt)("h2",{id:"string"},"String"),(0,i.kt)("p",null,"String literals are surrounded by single-quotes and may use ",(0,i.kt)("inlineCode",{parentName:"p"},"\\"),"-escapes to\nescape quotes and represent Unicode characters:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Unicode characters may be escaped using \\u followed by four hex digits."),(0,i.kt)("li",{parentName:"ul"},"Additional escapes are those supported in JSON:",(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"\\\\")," (backslash),"),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"\\/")," (slash),"),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"\\f")," (form feed - ",(0,i.kt)("inlineCode",{parentName:"li"},"\\u000c"),"),"),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"\\n")," (newline - ",(0,i.kt)("inlineCode",{parentName:"li"},"\\u000a"),"),"),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"\\r")," (carriage return - ",(0,i.kt)("inlineCode",{parentName:"li"},"\\u000d"),"),"),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"\\t")," (tab - ",(0,i.kt)("inlineCode",{parentName:"li"},"\\u0009"),")"),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("code",null,"\\``")," (backtick)"),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"\\'")," (single-quote)")))),(0,i.kt)("p",null,"Unicode is supported in both string literals and delimited identifiers."),(0,i.kt)("p",null,"Examples:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"'test string'\n'urn:oid:3.4.5.6.7.8'\n'M\\u00fcller'           // Includes a Unicode character, evaluates to M\xfcller\n")),(0,i.kt)("h2",{id:"integer"},"Integer"),(0,i.kt)("p",null,"The Integer type represents whole numbers."),(0,i.kt)("p",null,"Examples:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"352\n-14\n")),(0,i.kt)("h2",{id:"decimal"},"Decimal"),(0,i.kt)("p",null,"The Decimal type represents real values."),(0,i.kt)("p",null,"Examples:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"14.25\n-3.333\n")),(0,i.kt)("admonition",{type:"note"},(0,i.kt)("p",{parentName:"admonition"},"The implementation of Decimal within Pathling supports a precision of 32 and a\nscale of 6.")),(0,i.kt)("h2",{id:"datetime"},"DateTime"),(0,i.kt)("p",null,"The DateTime literal combines the ",(0,i.kt)("a",{parentName:"p",href:"#date"},"Date")," and ",(0,i.kt)("a",{parentName:"p",href:"#time"},"Time")," literals and\nis a subset of ",(0,i.kt)("a",{parentName:"p",href:"https://en.wikipedia.org/wiki/ISO_8601"},"ISO 8601"),". It uses the\n",(0,i.kt)("inlineCode",{parentName:"p"},"YYYY-MM-DDThh:mm:ss.ffff\xb1hh:mm")," format. ",(0,i.kt)("inlineCode",{parentName:"p"},"Z")," is allowed as a synonym for the\nzero (",(0,i.kt)("inlineCode",{parentName:"p"},"+00:00"),") UTC offset."),(0,i.kt)("p",null,"Time zone is optional - if it is omitted, the system-configured time zone will\nbe assumed. Seconds and milliseconds precision are supported. Hours precision,\nminutes precision and partial DateTime values (ending with ",(0,i.kt)("inlineCode",{parentName:"p"},"T"),") are not\nsupported."),(0,i.kt)("p",null,"Example:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"@2014-01-25T14:30:14         // Seconds precision\n@2014-01-25T14:30:14+10:00   // Seconds precision with UTC+10 timezone offset\n@2014-01-25T14:30:14.559     // Milliseconds precision\n@2014-01-25T14:30:14.559Z    // Milliseconds precision with UTC timezone offset\n")),(0,i.kt)("h2",{id:"date"},"Date"),(0,i.kt)("p",null,"The Date type represents date and partial date values, without a time component."),(0,i.kt)("p",null,"The Date literal is a subset of\n",(0,i.kt)("a",{parentName:"p",href:"https://en.wikipedia.org/wiki/ISO_8601"},"ISO 8601"),". It uses the ",(0,i.kt)("inlineCode",{parentName:"p"},"YYYY-MM-DD"),"\nformat, though month and day parts are optional."),(0,i.kt)("p",null,"Some operations implicitly convert Date values to DateTime values, such as\ncomparison and arithmetic. Note that the Date will be assumed to be in the\nsystem-configured time zone in these instances."),(0,i.kt)("p",null,"Examples:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"@2014-01-25    // Year, month and day\n@2014-01       // Year and month only\n@2014          // Year only\n")),(0,i.kt)("h2",{id:"time"},"Time"),(0,i.kt)("p",null,"The Time type represents time-of-day and partial time-of-day values."),(0,i.kt)("p",null,"The Time literal uses a subset of\n",(0,i.kt)("a",{parentName:"p",href:"https://en.wikipedia.org/wiki/ISO_8601"},"ISO 8601"),":"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"A time begins with a ",(0,i.kt)("inlineCode",{parentName:"li"},"@T")),(0,i.kt)("li",{parentName:"ul"},"It uses the ",(0,i.kt)("inlineCode",{parentName:"li"},"Thh:mm:ss.fff")," format, minutes, seconds and milliseconds are\noptional")),(0,i.kt)("p",null,"Examples:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"@T07:30:14.350    // Milliseconds precision\n@T07:30:14        // Seconds precision\n@T14:30:14        // Minutes precision\n@T14              // Hours precision\n")),(0,i.kt)("h2",{id:"quantity"},"Quantity"),(0,i.kt)("p",null,"The Quantity type represents quantities with a specified unit, where the value\ncomponent is defined as a Decimal, and the unit element is represented as a\nString that is required to be either a valid\n",(0,i.kt)("a",{parentName:"p",href:"https://ucum.org/ucum.html"},"Unified Code for Units of Measure (UCUM)")," unit or\none of the calendar duration keywords, singular or plural."),(0,i.kt)("p",null,"The Quantity literal is a number (integer or decimal), followed by a\n(single-quoted) string representing a valid UCUM unit or calendar duration\nkeyword. If the value literal is an Integer, it will be implicitly converted to\na Decimal in the resulting Quantity value."),(0,i.kt)("p",null,"The calendar duration keywords that are supported are:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"year")," / ",(0,i.kt)("inlineCode",{parentName:"li"},"years")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"month")," / ",(0,i.kt)("inlineCode",{parentName:"li"},"months")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"week")," / ",(0,i.kt)("inlineCode",{parentName:"li"},"weeks")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"day")," / ",(0,i.kt)("inlineCode",{parentName:"li"},"days")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"hour")," / ",(0,i.kt)("inlineCode",{parentName:"li"},"hours")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"minute")," / ",(0,i.kt)("inlineCode",{parentName:"li"},"minutes")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"second")," / ",(0,i.kt)("inlineCode",{parentName:"li"},"seconds")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"millisecond")," / ",(0,i.kt)("inlineCode",{parentName:"li"},"milliseconds"))),(0,i.kt)("p",null,"Example:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"4.5 'mg'      // UCUM Quantity of 4.5 mg\n100 '[degF]'  // UCUM Quantity of 100 degrees Fahrenheit\n6 months      // Calendar duration of 6 months\n30 days       // Calendar duration of 30 days\n")),(0,i.kt)("p",null,"See: ",(0,i.kt)("a",{parentName:"p",href:"https://hl7.org/fhirpath/#quantity"},"Quantity")),(0,i.kt)("h2",{id:"coding"},"Coding"),(0,i.kt)("p",null,"A ",(0,i.kt)("a",{parentName:"p",href:"https://hl7.org/fhir/R4/datatypes.html#Coding"},"Coding")," is a representation of\na defined concept using a symbol from a defined\n",(0,i.kt)("a",{parentName:"p",href:"https://hl7.org/fhir/R4/codesystem.html"},"code system")," - see\n",(0,i.kt)("a",{parentName:"p",href:"https://hl7.org/fhir/R4/terminologies.html"},"Using Codes in resources")," for more\ndetails."),(0,i.kt)("p",null,"The Coding literal comprises a minimum of ",(0,i.kt)("inlineCode",{parentName:"p"},"system")," and ",(0,i.kt)("inlineCode",{parentName:"p"},"code"),", as well as\noptional ",(0,i.kt)("inlineCode",{parentName:"p"},"version"),", ",(0,i.kt)("inlineCode",{parentName:"p"},"display"),", ",(0,i.kt)("inlineCode",{parentName:"p"},"userSelected")," components:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"<system>|<code>[|<version>][|<display>[|<userSelected>]]]\n")),(0,i.kt)("p",null,"Not all code systems require the use of a version to unambiguously specify a\ncode - see\n",(0,i.kt)("a",{parentName:"p",href:"https://hl7.org/fhir/R4/codesystem.html#versioning"},"Versioning Code Systems"),"."),(0,i.kt)("p",null,"You can also optionally single-quote each of the components within the Coding\nliteral, in cases where certain characters might otherwise confuse the parser."),(0,i.kt)("p",null,"Examples:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"http://snomed.info/sct|52101004\nhttp://snomed.info/sct|52101004||Present\nhttp://terminology.hl7.org/CodeSystem/condition-category|problem-list-item|4.0.1|'Problem List Item'\nhttp://snomed.info/sct|'397956004 |Prosthetic arthroplasty of the hip|: 363704007 |Procedure site| = ( 24136001 |Hip joint structure|: 272741003 |Laterality| =  7771000 |Left| )'\n")),(0,i.kt)("admonition",{type:"note"},(0,i.kt)("p",{parentName:"admonition"},"The Coding literal is not within the FHIRPath specification, and is currently\nunique to the Pathling implementation.")),(0,i.kt)("h2",{id:"materializable-types"},"Materializable types"),(0,i.kt)("p",null,'There is a subset of all possible FHIR types that can be "materialized", i.e.\nused as the result of an aggregation or grouping expression in\nthe ',(0,i.kt)("a",{parentName:"p",href:"/docs/server/operations/aggregate"},"aggregate"),"\noperation, or a column expression within\nthe ",(0,i.kt)("a",{parentName:"p",href:"/docs/server/operations/extract"},"extract"),"\noperation. These types are:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("a",{parentName:"li",href:"#boolean"},"Boolean")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("a",{parentName:"li",href:"#string"},"String")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("a",{parentName:"li",href:"#integer"},"Integer")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("a",{parentName:"li",href:"#decimal"},"Decimal")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("a",{parentName:"li",href:"#date"},"Date")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("a",{parentName:"li",href:"#datetime"},"DateTime")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("a",{parentName:"li",href:"#time"},"Time")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("a",{parentName:"li",href:"#coding"},"Coding"))))}u.isMDXComponent=!0}}]);