"use strict";(self.webpackChunkpathling_site=self.webpackChunkpathling_site||[]).push([[850],{3905:(e,t,n)=>{n.d(t,{Zo:()=>p,kt:()=>f});var r=n(7294);function a(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){a(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,r,a=function(e,t){if(null==e)return{};var n,r,a={},i=Object.keys(e);for(r=0;r<i.length;r++)n=i[r],t.indexOf(n)>=0||(a[n]=e[n]);return a}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(r=0;r<i.length;r++)n=i[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var c=r.createContext({}),l=function(e){var t=r.useContext(c),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},p=function(e){var t=l(e.components);return r.createElement(c.Provider,{value:t},e.children)},h="mdxType",u={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},d=r.forwardRef((function(e,t){var n=e.components,a=e.mdxType,i=e.originalType,c=e.parentName,p=s(e,["components","mdxType","originalType","parentName"]),h=l(n),d=a,f=h["".concat(c,".").concat(d)]||h[d]||u[d]||i;return n?r.createElement(f,o(o({ref:t},p),{},{components:n})):r.createElement(f,o({ref:t},p))}));function f(e,t){var n=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var i=n.length,o=new Array(i);o[0]=d;var s={};for(var c in t)hasOwnProperty.call(t,c)&&(s[c]=t[c]);s.originalType=e,s[h]="string"==typeof e?e:a,o[1]=s;for(var l=2;l<i;l++)o[l]=n[l];return r.createElement.apply(null,o)}return r.createElement.apply(null,n)}d.displayName="MDXCreateElement"},8448:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>c,contentTitle:()=>o,default:()=>u,frontMatter:()=>i,metadata:()=>s,toc:()=>l});var r=n(7462),a=(n(7294),n(3905));const i={sidebar_position:8},o="Caching",s={unversionedId:"server/caching",id:"server/caching",title:"Caching",description:"Pathling implements",source:"@site/docs/server/caching.md",sourceDirName:"server",slug:"/server/caching",permalink:"/docs/server/caching",draft:!1,editUrl:"https://github.com/aehrc/pathling/tree/main/site/docs/server/caching.md",tags:[],version:"current",sidebarPosition:8,frontMatter:{sidebar_position:8},sidebar:"server",previous:{title:"Async",permalink:"/docs/server/async"},next:{title:"Synchronization",permalink:"/docs/server/sync"}},c={},l=[],p={toc:l},h="wrapper";function u(e){let{components:t,...n}=e;return(0,a.kt)(h,(0,r.Z)({},p,n,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h1",{id:"caching"},"Caching"),(0,a.kt)("p",null,"Pathling implements\n",(0,a.kt)("a",{parentName:"p",href:"https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag"},"ETag"),"-based\ncache validation, which enables clients to skip processing of queries when the\nunderlying data has not changed."),(0,a.kt)("p",null,"To use ETags, simply take the content of the ",(0,a.kt)("inlineCode",{parentName:"p"},"ETag")," header that is returned with\na Pathling response. You can then accompany a subsequent request with the\n",(0,a.kt)("a",{parentName:"p",href:"https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/If-None-Match"},"If-None-Match"),",\nusing the previously received ETag as the value. If the result of the query\nwould not have changed, Pathling will respond\nwith ",(0,a.kt)("a",{parentName:"p",href:"https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/304"},"304 Not Modified"),",\nand will skip re-processing of the query."),(0,a.kt)("p",null,"Web browsers already implement this behaviour, and if your application runs in\nthe browser you will get the benefits without any extra implementation effort."),(0,a.kt)("p",null,"The current limitation of this implementation within Pathling is that caching is\ndone over the entire database, not scoped to individual resource types. This\nmeans that updates to any resource will invalidate the cache for the entire\ndatabase."),(0,a.kt)("p",null,"Cache keys persist across restarts of the server, as they are derived from state\nthat is persisted along with the data."))}u.isMDXComponent=!0}}]);