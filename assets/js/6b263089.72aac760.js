"use strict";(self.webpackChunkpathling_site=self.webpackChunkpathling_site||[]).push([[442],{3905:(e,t,n)=>{n.d(t,{Zo:()=>p,kt:()=>m});var i=n(7294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function a(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);t&&(i=i.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,i)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?a(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):a(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,i,r=function(e,t){if(null==e)return{};var n,i,r={},a=Object.keys(e);for(i=0;i<a.length;i++)n=a[i],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(i=0;i<a.length;i++)n=a[i],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var s=i.createContext({}),c=function(e){var t=i.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},p=function(e){var t=c(e.components);return i.createElement(s.Provider,{value:t},e.children)},d="mdxType",u={inlineCode:"code",wrapper:function(e){var t=e.children;return i.createElement(i.Fragment,{},t)}},h=i.forwardRef((function(e,t){var n=e.components,r=e.mdxType,a=e.originalType,s=e.parentName,p=l(e,["components","mdxType","originalType","parentName"]),d=c(n),h=r,m=d["".concat(s,".").concat(h)]||d[h]||u[h]||a;return n?i.createElement(m,o(o({ref:t},p),{},{components:n})):i.createElement(m,o({ref:t},p))}));function m(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var a=n.length,o=new Array(a);o[0]=h;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l[d]="string"==typeof e?e:r,o[1]=l;for(var c=2;c<a;c++)o[c]=n[c];return i.createElement.apply(null,o)}return i.createElement.apply(null,n)}h.displayName="MDXCreateElement"},4453:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>s,contentTitle:()=>o,default:()=>u,frontMatter:()=>a,metadata:()=>l,toc:()=>c});var i=n(7462),r=(n(7294),n(3905));const a={sidebar_position:2,description:"Instructions for installing the Pathling libraries on Windows."},o="Windows installation",l={unversionedId:"libraries/installation/windows",id:"libraries/installation/windows",title:"Windows installation",description:"Instructions for installing the Pathling libraries on Windows.",source:"@site/docs/libraries/installation/windows.md",sourceDirName:"libraries/installation",slug:"/libraries/installation/windows",permalink:"/docs/libraries/installation/windows",draft:!1,editUrl:"https://github.com/aehrc/pathling/tree/main/site/docs/libraries/installation/windows.md",tags:[],version:"current",sidebarPosition:2,frontMatter:{sidebar_position:2,description:"Instructions for installing the Pathling libraries on Windows."},sidebar:"libraries",previous:{title:"Installation",permalink:"/docs/libraries/installation/"},next:{title:"Databricks installation",permalink:"/docs/libraries/installation/databricks"}},s={},c=[],p={toc:c},d="wrapper";function u(e){let{components:t,...n}=e;return(0,r.kt)(d,(0,i.Z)({},p,n,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h1",{id:"windows-installation"},"Windows installation"),(0,r.kt)("p",null,"Installing Pathling (or Apache Spark) on Windows requires a few extra steps.\nThis is because the Hadoop libraries that Spark depends upon require special\nbinaries to run on Windows."),(0,r.kt)("p",null,"To solve this problem, you can do the following:"),(0,r.kt)("ol",null,(0,r.kt)("li",{parentName:"ol"},"Go to the ",(0,r.kt)("a",{parentName:"li",href:"https://github.com/steveloughran/winutils"},"winutils"),' repository,\nclick the "Code" button and download the latest release as a ZIP file.'),(0,r.kt)("li",{parentName:"ol"},"Extract the downloaded archive to a directory of your choice. For example,\nyou can extract it to ",(0,r.kt)("inlineCode",{parentName:"li"},"C:\\hadoop"),"."),(0,r.kt)("li",{parentName:"ol"},"Set the ",(0,r.kt)("inlineCode",{parentName:"li"},"HADOOP_HOME")," environment variable to the ",(0,r.kt)("inlineCode",{parentName:"li"},"hadoop-3.0.0")," subdirectory\nwithin the installation directory, e.g. ",(0,r.kt)("inlineCode",{parentName:"li"},"c:\\hadoop\\hadoop-3.0.0"),"."),(0,r.kt)("li",{parentName:"ol"},"Add ",(0,r.kt)("inlineCode",{parentName:"li"},"%HADOOP_HOME%\\bin")," to the ",(0,r.kt)("inlineCode",{parentName:"li"},"PATH")," environment variable.")),(0,r.kt)("p",null,'You can set an environment variable in Windows by right-clicking on "This PC"\nand selecting "Properties", then clicking on "Advanced system settings", then\nclicking on "Environment Variables". You will need to use "System variables",\nrather than "User variables". You will need administrator privileges on\nthe computer to be able to do this.'),(0,r.kt)("p",null,"To verify that the variables have been set correctly, open a new command prompt\nand run:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-cmd"},"dir %HADOOP_HOME%\nhadoop\n")),(0,r.kt)("p",null,"If this executes successfully, finding the Hadoop executable and printing the\nhelp text, then the variables have been set correctly."),(0,r.kt)("p",null,"As with other operating systems, you also need to have Java 11 installed and\nthe ",(0,r.kt)("inlineCode",{parentName:"p"},"JAVA_HOME")," environment variable set to its installation location. We\nrecommend\nthe ",(0,r.kt)("a",{parentName:"p",href:"https://www.azul.com/downloads/?version=java-11-lts&os=windows&package=jdk#zulu"},"Azul Zulu installer for Windows"),"."))}u.isMDXComponent=!0}}]);