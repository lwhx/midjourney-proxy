!function(){"use strict";var t="".replace(/([^/])$/,"$1/"),e=location.pathname,n=e.startsWith(t)&&decodeURI("/".concat(e.slice(t.length)));if(n){var a=document,c=a.head,r=a.createElement.bind(a),i=function(t,e,n){var a,c=e.r[t]||(null===(a=Object.entries(e.r).find((function(e){var n=e[0];return new RegExp("^".concat(n.replace(/\/:[^/]+/g,"/[^/]+").replace("/*","/.+"),"$")).test(t)})))||void 0===a?void 0:a[1]);return null==c?void 0:c.map((function(t){var a=e.f[t][1],c=e.f[t][0];return{type:c.split(".").pop(),url:"".concat(n.publicPath).concat(c),attrs:[["data-".concat(e.b),"".concat(e.p,":").concat(a)]]}}))}(n,{"p":"midjourney-proxy-admin","b":"webpack","f":[["13.163d1ed9.async.js",13],["74.33c099e0.async.js",74],["120.e1c47d8f.async.js",120],["121.a9c92295.async.js",121],["134.2716c782.async.js",134],["141.9917004a.async.js",141],["178.7d1d06a1.async.js",178],["p__Welcome.21339f9d.async.js",185],["p__AccountList__index.63614185.async.js",201],["228.2ee81a73.async.js",228],["248.3624d45a.async.js",248],["t__plugin-layout__Layout.5012e1ab.chunk.css",301],["t__plugin-layout__Layout.90399cd0.async.js",301],["p__User__Login__index.5adf3dd4.async.js",366],["390.aaa3ec24.async.js",390],["425.53080ddf.async.js",425],["p__Task__List__index.a8b44174.async.js",502],["p__DomainList__List__index.3e055596.async.js",523],["p__404.d82f6ec8.async.js",571],["p__UserList__List__index.1f3fb498.async.js",579],["597.a1412452.async.js",597],["p__BannedWordList__List__index.70f67384.async.js",636],["643.91d4cb9a.async.js",643],["p__Probe__index.6a9ff373.chunk.css",780],["p__Probe__index.f9b62a0e.async.js",780],["p__Draw__index.63774c9e.chunk.css",903],["p__Draw__index.abd9c277.async.js",903],["905.5387c98a.async.js",905],["915.c1021c3b.async.js",915],["p__Setting__index.20a94653.async.js",971]],"r":{"/*":[18,27],"/":[4,11,12,15,27],"/welcome":[4,7,9,11,12,15,27],"/account":[0,3,4,5,6,8,9,10,20,27,28,11,12,15],"/domain":[0,4,5,6,9,10,17,20,27,28,11,12,15],"/user-list":[0,4,5,6,9,10,19,20,27,28,11,12,15],"/task":[0,4,5,6,9,10,16,20,22,27,28,11,12,15],"/draw-test":[1,4,9,10,20,25,26,28,11,12,15,27],"/banned-word":[0,4,5,6,9,10,20,21,27,28,11,12,15],"/setting":[0,2,4,9,28,29,11,12,15,27],"/probe":[4,9,23,24,11,12,15,27],"/user/login":[0,4,5,13,20,28]}},{publicPath:"/"});null==i||i.forEach((function(t){var e,n=t.type,a=t.url;if("js"===n)(e=r("script")).src=a,e.async=!0;else{if("css"!==n)return;(e=r("link")).href=a,e.rel="preload",e.as="style"}t.attrs.forEach((function(t){e.setAttribute(t[0],t[1]||"")})),c.appendChild(e)}))}}();