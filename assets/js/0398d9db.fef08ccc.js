"use strict";(self.webpackChunkpathling_site=self.webpackChunkpathling_site||[]).push([[429],{3905:(e,t,n)=>{n.d(t,{Zo:()=>l,kt:()=>d});var r=n(7294);function o(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function a(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?a(Object(n),!0).forEach((function(t){o(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):a(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,r,o=function(e,t){if(null==e)return{};var n,r,o={},a=Object.keys(e);for(r=0;r<a.length;r++)n=a[r],t.indexOf(n)>=0||(o[n]=e[n]);return o}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(r=0;r<a.length;r++)n=a[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(o[n]=e[n])}return o}var c=r.createContext({}),p=function(e){var t=r.useContext(c),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},l=function(e){var t=p(e.components);return r.createElement(c.Provider,{value:t},e.children)},u="mdxType",h={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},m=r.forwardRef((function(e,t){var n=e.components,o=e.mdxType,a=e.originalType,c=e.parentName,l=s(e,["components","mdxType","originalType","parentName"]),u=p(n),m=o,d=u["".concat(c,".").concat(m)]||u[m]||h[m]||a;return n?r.createElement(d,i(i({ref:t},l),{},{components:n})):r.createElement(d,i({ref:t},l))}));function d(e,t){var n=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var a=n.length,i=new Array(a);i[0]=m;var s={};for(var c in t)hasOwnProperty.call(t,c)&&(s[c]=t[c]);s.originalType=e,s[u]="string"==typeof e?e:o,i[1]=s;for(var p=2;p<a;p++)i[p]=n[p];return r.createElement.apply(null,i)}return r.createElement.apply(null,n)}m.displayName="MDXCreateElement"},9217:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>c,contentTitle:()=>i,default:()=>h,frontMatter:()=>a,metadata:()=>s,toc:()=>p});var r=n(7462),o=(n(7294),n(3905));const a={sidebar_position:7,sidebar_label:"Async",description:"Pathling implements the Asynchronous Request Pattern within the FHIR specification, to provide a way to execute long-running requests and check on their progress using a status endpoint."},i="Asynchronous processing",s={unversionedId:"server/async",id:"server/async",title:"Asynchronous processing",description:"Pathling implements the Asynchronous Request Pattern within the FHIR specification, to provide a way to execute long-running requests and check on their progress using a status endpoint.",source:"@site/docs/server/async.md",sourceDirName:"server",slug:"/server/async",permalink:"/docs/server/async",draft:!1,editUrl:"https://github.com/aehrc/pathling/tree/main/site/docs/server/async.md",tags:[],version:"current",sidebarPosition:7,frontMatter:{sidebar_position:7,sidebar_label:"Async",description:"Pathling implements the Asynchronous Request Pattern within the FHIR specification, to provide a way to execute long-running requests and check on their progress using a status endpoint."},sidebar:"server",previous:{title:"Authorization",permalink:"/docs/server/authorization"},next:{title:"Caching",permalink:"/docs/server/caching"}},c={},p=[{value:"Examples",id:"examples",level:2}],l={toc:p},u="wrapper";function h(e){let{components:t,...n}=e;return(0,o.kt)(u,(0,r.Z)({},l,n,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h1",{id:"asynchronous-processing"},"Asynchronous processing"),(0,o.kt)("p",null,"Pathling implements\nthe ",(0,o.kt)("a",{parentName:"p",href:"https://hl7.org/fhir/R4/async.html"},"Asynchronous Request Pattern"),"\nwithin the FHIR specification, to provide a way to execute long-running requests\nand check on their progress using a status endpoint."),(0,o.kt)("p",null,"Add a ",(0,o.kt)("inlineCode",{parentName:"p"},"Prefer: respond-async")," header to your request to indicate that you want\nto use asynchronous processing. A ",(0,o.kt)("inlineCode",{parentName:"p"},"202 Accepted")," response will be returned,\nalong with a ",(0,o.kt)("inlineCode",{parentName:"p"},"Content-Location")," header indicating the URL of the status\nendpoint."),(0,o.kt)("p",null,"The following operations support async:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("a",{parentName:"li",href:"./operations/import"},"Import")),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("a",{parentName:"li",href:"./operations/aggregate"},"Aggregate")),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("a",{parentName:"li",href:"./operations/extract"},"Extract"))),(0,o.kt)("p",null,"Async job references are stored in memory. If the server is restarted before\nthe completion of the job, the initiation request will need to be resent."),(0,o.kt)("p",null,"If you're using JavaScript, you can use\nthe ",(0,o.kt)("a",{parentName:"p",href:"https://www.npmjs.com/package/pathling-client"},"pathling-client")," NPM package\nwhich features support for calling Pathling operations using the async pattern."),(0,o.kt)("h2",{id:"examples"},"Examples"),(0,o.kt)("p",null,"Check out example async requests in the Postman collection:"),(0,o.kt)("a",{class:"postman-link",href:"https://documenter.getpostman.com/view/634774/UVsQs48s#dd55e70a-c67c-4132-9a80-ab5f69e8c2ff"},(0,o.kt)("img",{src:"https://run.pstmn.io/button.svg",alt:"Run in Postman"})))}h.isMDXComponent=!0}}]);