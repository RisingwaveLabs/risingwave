"use strict";(self.webpackChunk_N_E=self.webpackChunk_N_E||[]).push([[851],{56103:function(r,e,t){var o=t(85893),n=t(40639);t(67294),e.Z=function(r){let{children:e}=r;return(0,o.jsx)(n.xv,{mb:2,textColor:"blue.500",fontWeight:"semibold",lineHeight:"6",children:e})}},51388:function(r,e,t){t.d(e,{Z:function(){return a}});var o=t(66479),n=t(67294);function a(){let r=(0,o.pm)();return(0,n.useCallback)(function(e){let t,o,n=arguments.length>1&&void 0!==arguments[1]?arguments[1]:"error";if(e instanceof Error){var a;t=e.message,o=null===(a=e.cause)||void 0===a?void 0:a.toString()}else t=e.toString();r({title:t,description:o,status:n,duration:5e3,isClosable:!0})},[r])}},29286:function(r,e,t){t.d(e,{Cm:function(){return E},Yg:function(){return a},e_:function(){return n}});let o="/api",n=[o,"http://localhost:32333","http://localhost:5691/api"],a=o,E="risingwave.dashboard.api.endpoint";class l{urlFor(r){let e=(JSON.parse(localStorage.getItem(E)||"null")||a).replace(/\/+$/,"");return"".concat(e).concat(r)}async get(r){let e=this.urlFor(r);try{let r=await fetch(e),t=await r.json();if(!r.ok)throw"".concat(r.status," ").concat(r.statusText).concat(t.error?": "+t.error:"");return t}catch(r){throw console.error(r),Error("Failed to fetch ".concat(e),{cause:r})}}}let i=new l;e.ZP=i},56250:function(r,e,t){t.d(e,{E9:function(){return R},GS:function(){return S},L3:function(){return o},Tq:function(){return O},XE:function(){return I},XK:function(){return n},Yz:function(){return T},cX:function(){return d},i4:function(){return c},lW:function(){return C},mr:function(){return _},qb:function(){return u},u:function(){return a}});let o={WORKER_TYPE_UNSPECIFIED:"WORKER_TYPE_UNSPECIFIED",WORKER_TYPE_FRONTEND:"WORKER_TYPE_FRONTEND",WORKER_TYPE_COMPUTE_NODE:"WORKER_TYPE_COMPUTE_NODE",WORKER_TYPE_RISE_CTL:"WORKER_TYPE_RISE_CTL",WORKER_TYPE_COMPACTOR:"WORKER_TYPE_COMPACTOR",WORKER_TYPE_META:"WORKER_TYPE_META",UNRECOGNIZED:"UNRECOGNIZED"};function n(r){switch(r){case 0:case"WORKER_TYPE_UNSPECIFIED":return o.WORKER_TYPE_UNSPECIFIED;case 1:case"WORKER_TYPE_FRONTEND":return o.WORKER_TYPE_FRONTEND;case 2:case"WORKER_TYPE_COMPUTE_NODE":return o.WORKER_TYPE_COMPUTE_NODE;case 3:case"WORKER_TYPE_RISE_CTL":return o.WORKER_TYPE_RISE_CTL;case 4:case"WORKER_TYPE_COMPACTOR":return o.WORKER_TYPE_COMPACTOR;case 5:case"WORKER_TYPE_META":return o.WORKER_TYPE_META;default:return o.UNRECOGNIZED}}function a(r){switch(r){case o.WORKER_TYPE_UNSPECIFIED:return"WORKER_TYPE_UNSPECIFIED";case o.WORKER_TYPE_FRONTEND:return"WORKER_TYPE_FRONTEND";case o.WORKER_TYPE_COMPUTE_NODE:return"WORKER_TYPE_COMPUTE_NODE";case o.WORKER_TYPE_RISE_CTL:return"WORKER_TYPE_RISE_CTL";case o.WORKER_TYPE_COMPACTOR:return"WORKER_TYPE_COMPACTOR";case o.WORKER_TYPE_META:return"WORKER_TYPE_META";case o.UNRECOGNIZED:default:return"UNRECOGNIZED"}}let E={DIRECTION_UNSPECIFIED:"DIRECTION_UNSPECIFIED",DIRECTION_ASCENDING:"DIRECTION_ASCENDING",DIRECTION_DESCENDING:"DIRECTION_DESCENDING",UNRECOGNIZED:"UNRECOGNIZED"},l={NULLS_ARE_UNSPECIFIED:"NULLS_ARE_UNSPECIFIED",NULLS_ARE_LARGEST:"NULLS_ARE_LARGEST",NULLS_ARE_SMALLEST:"NULLS_ARE_SMALLEST",UNRECOGNIZED:"UNRECOGNIZED"},i={UNSPECIFIED:"UNSPECIFIED",OK:"OK",UNKNOWN_WORKER:"UNKNOWN_WORKER",UNRECOGNIZED:"UNRECOGNIZED"},s={UNSPECIFIED:"UNSPECIFIED",STARTING:"STARTING",RUNNING:"RUNNING",UNRECOGNIZED:"UNRECOGNIZED"},N={UNSPECIFIED:"UNSPECIFIED",NONE:"NONE",UNRECOGNIZED:"UNRECOGNIZED"},u={fromJSON:r=>({code:U(r.code)?function(r){switch(r){case 0:case"UNSPECIFIED":return i.UNSPECIFIED;case 1:case"OK":return i.OK;case 2:case"UNKNOWN_WORKER":return i.UNKNOWN_WORKER;default:return i.UNRECOGNIZED}}(r.code):i.UNSPECIFIED,message:U(r.message)?globalThis.String(r.message):""}),toJSON(r){let e={};return r.code!==i.UNSPECIFIED&&(e.code=function(r){switch(r){case i.UNSPECIFIED:return"UNSPECIFIED";case i.OK:return"OK";case i.UNKNOWN_WORKER:return"UNKNOWN_WORKER";case i.UNRECOGNIZED:default:return"UNRECOGNIZED"}}(r.code)),""!==r.message&&(e.message=r.message),e},create:r=>u.fromPartial(null!=r?r:{}),fromPartial(r){var e,t;let o={code:i.UNSPECIFIED,message:""};return o.code=null!==(e=r.code)&&void 0!==e?e:i.UNSPECIFIED,o.message=null!==(t=r.message)&&void 0!==t?t:"",o}},I={fromJSON:r=>({host:U(r.host)?globalThis.String(r.host):"",port:U(r.port)?globalThis.Number(r.port):0}),toJSON(r){let e={};return""!==r.host&&(e.host=r.host),0!==r.port&&(e.port=Math.round(r.port)),e},create:r=>I.fromPartial(null!=r?r:{}),fromPartial(r){var e,t;let o={host:"",port:0};return o.host=null!==(e=r.host)&&void 0!==e?e:"",o.port=null!==(t=r.port)&&void 0!==t?t:0,o}},c={fromJSON:r=>({workerNodeId:U(r.workerNodeId)?globalThis.Number(r.workerNodeId):0}),toJSON(r){let e={};return 0!==r.workerNodeId&&(e.workerNodeId=Math.round(r.workerNodeId)),e},create:r=>c.fromPartial(null!=r?r:{}),fromPartial(r){var e;let t={workerNodeId:0};return t.workerNodeId=null!==(e=r.workerNodeId)&&void 0!==e?e:0,t}},d={fromJSON:r=>({id:U(r.id)?globalThis.Number(r.id):0,type:U(r.type)?n(r.type):o.WORKER_TYPE_UNSPECIFIED,host:U(r.host)?I.fromJSON(r.host):void 0,state:U(r.state)?function(r){switch(r){case 0:case"UNSPECIFIED":return s.UNSPECIFIED;case 1:case"STARTING":return s.STARTING;case 2:case"RUNNING":return s.RUNNING;default:return s.UNRECOGNIZED}}(r.state):s.UNSPECIFIED,property:U(r.property)?R.fromJSON(r.property):void 0,transactionalId:U(r.transactionalId)?globalThis.Number(r.transactionalId):void 0,resource:U(r.resource)?S.fromJSON(r.resource):void 0,startedAt:U(r.startedAt)?globalThis.Number(r.startedAt):void 0}),toJSON(r){let e={};return 0!==r.id&&(e.id=Math.round(r.id)),r.type!==o.WORKER_TYPE_UNSPECIFIED&&(e.type=a(r.type)),void 0!==r.host&&(e.host=I.toJSON(r.host)),r.state!==s.UNSPECIFIED&&(e.state=function(r){switch(r){case s.UNSPECIFIED:return"UNSPECIFIED";case s.STARTING:return"STARTING";case s.RUNNING:return"RUNNING";case s.UNRECOGNIZED:default:return"UNRECOGNIZED"}}(r.state)),void 0!==r.property&&(e.property=R.toJSON(r.property)),void 0!==r.transactionalId&&(e.transactionalId=Math.round(r.transactionalId)),void 0!==r.resource&&(e.resource=S.toJSON(r.resource)),void 0!==r.startedAt&&(e.startedAt=Math.round(r.startedAt)),e},create:r=>d.fromPartial(null!=r?r:{}),fromPartial(r){var e,t,n,a,E;let l={id:0,type:o.WORKER_TYPE_UNSPECIFIED,host:void 0,state:s.UNSPECIFIED,property:void 0,transactionalId:void 0,resource:void 0,startedAt:void 0};return l.id=null!==(e=r.id)&&void 0!==e?e:0,l.type=null!==(t=r.type)&&void 0!==t?t:o.WORKER_TYPE_UNSPECIFIED,l.host=void 0!==r.host&&null!==r.host?I.fromPartial(r.host):void 0,l.state=null!==(n=r.state)&&void 0!==n?n:s.UNSPECIFIED,l.property=void 0!==r.property&&null!==r.property?R.fromPartial(r.property):void 0,l.transactionalId=null!==(a=r.transactionalId)&&void 0!==a?a:void 0,l.resource=void 0!==r.resource&&null!==r.resource?S.fromPartial(r.resource):void 0,l.startedAt=null!==(E=r.startedAt)&&void 0!==E?E:void 0,l}},R={fromJSON:r=>({isStreaming:!!U(r.isStreaming)&&globalThis.Boolean(r.isStreaming),isServing:!!U(r.isServing)&&globalThis.Boolean(r.isServing),isUnschedulable:!!U(r.isUnschedulable)&&globalThis.Boolean(r.isUnschedulable),internalRpcHostAddr:U(r.internalRpcHostAddr)?globalThis.String(r.internalRpcHostAddr):"",parallelism:U(r.parallelism)?globalThis.Number(r.parallelism):0,resourceGroup:U(r.resourceGroup)?globalThis.String(r.resourceGroup):void 0}),toJSON(r){let e={};return!1!==r.isStreaming&&(e.isStreaming=r.isStreaming),!1!==r.isServing&&(e.isServing=r.isServing),!1!==r.isUnschedulable&&(e.isUnschedulable=r.isUnschedulable),""!==r.internalRpcHostAddr&&(e.internalRpcHostAddr=r.internalRpcHostAddr),0!==r.parallelism&&(e.parallelism=Math.round(r.parallelism)),void 0!==r.resourceGroup&&(e.resourceGroup=r.resourceGroup),e},create:r=>R.fromPartial(null!=r?r:{}),fromPartial(r){var e,t,o,n,a,E;let l={isStreaming:!1,isServing:!1,isUnschedulable:!1,internalRpcHostAddr:"",parallelism:0,resourceGroup:void 0};return l.isStreaming=null!==(e=r.isStreaming)&&void 0!==e&&e,l.isServing=null!==(t=r.isServing)&&void 0!==t&&t,l.isUnschedulable=null!==(o=r.isUnschedulable)&&void 0!==o&&o,l.internalRpcHostAddr=null!==(n=r.internalRpcHostAddr)&&void 0!==n?n:"",l.parallelism=null!==(a=r.parallelism)&&void 0!==a?a:0,l.resourceGroup=null!==(E=r.resourceGroup)&&void 0!==E?E:void 0,l}},S={fromJSON:r=>({rwVersion:U(r.rwVersion)?globalThis.String(r.rwVersion):"",totalMemoryBytes:U(r.totalMemoryBytes)?globalThis.Number(r.totalMemoryBytes):0,totalCpuCores:U(r.totalCpuCores)?globalThis.Number(r.totalCpuCores):0}),toJSON(r){let e={};return""!==r.rwVersion&&(e.rwVersion=r.rwVersion),0!==r.totalMemoryBytes&&(e.totalMemoryBytes=Math.round(r.totalMemoryBytes)),0!==r.totalCpuCores&&(e.totalCpuCores=Math.round(r.totalCpuCores)),e},create:r=>S.fromPartial(null!=r?r:{}),fromPartial(r){var e,t,o;let n={rwVersion:"",totalMemoryBytes:0,totalCpuCores:0};return n.rwVersion=null!==(e=r.rwVersion)&&void 0!==e?e:"",n.totalMemoryBytes=null!==(t=r.totalMemoryBytes)&&void 0!==t?t:0,n.totalCpuCores=null!==(o=r.totalCpuCores)&&void 0!==o?o:0,n}},C={fromJSON:r=>({compression:U(r.compression)?function(r){switch(r){case 0:case"UNSPECIFIED":return N.UNSPECIFIED;case 1:case"NONE":return N.NONE;default:return N.UNRECOGNIZED}}(r.compression):N.UNSPECIFIED,body:U(r.body)?function(r){if(globalThis.Buffer)return Uint8Array.from(globalThis.Buffer.from(r,"base64"));{let e=globalThis.atob(r),t=new Uint8Array(e.length);for(let r=0;r<e.length;++r)t[r]=e.charCodeAt(r);return t}}(r.body):new Uint8Array(0)}),toJSON(r){let e={};return r.compression!==N.UNSPECIFIED&&(e.compression=function(r){switch(r){case N.UNSPECIFIED:return"UNSPECIFIED";case N.NONE:return"NONE";case N.UNRECOGNIZED:default:return"UNRECOGNIZED"}}(r.compression)),0!==r.body.length&&(e.body=function(r){if(globalThis.Buffer)return globalThis.Buffer.from(r).toString("base64");{let e=[];return r.forEach(r=>{e.push(globalThis.String.fromCharCode(r))}),globalThis.btoa(e.join(""))}}(r.body)),e},create:r=>C.fromPartial(null!=r?r:{}),fromPartial(r){var e,t;let o={compression:N.UNSPECIFIED,body:new Uint8Array(0)};return o.compression=null!==(e=r.compression)&&void 0!==e?e:N.UNSPECIFIED,o.body=null!==(t=r.body)&&void 0!==t?t:new Uint8Array(0),o}},O={fromJSON:r=>({originalIndices:globalThis.Array.isArray(null==r?void 0:r.originalIndices)?r.originalIndices.map(r=>globalThis.Number(r)):[],data:globalThis.Array.isArray(null==r?void 0:r.data)?r.data.map(r=>globalThis.Number(r)):[]}),toJSON(r){var e,t;let o={};return(null===(e=r.originalIndices)||void 0===e?void 0:e.length)&&(o.originalIndices=r.originalIndices.map(r=>Math.round(r))),(null===(t=r.data)||void 0===t?void 0:t.length)&&(o.data=r.data.map(r=>Math.round(r))),o},create:r=>O.fromPartial(null!=r?r:{}),fromPartial(r){var e,t;let o={originalIndices:[],data:[]};return o.originalIndices=(null===(e=r.originalIndices)||void 0===e?void 0:e.map(r=>r))||[],o.data=(null===(t=r.data)||void 0===t?void 0:t.map(r=>r))||[],o}},_={fromJSON:r=>({direction:U(r.direction)?function(r){switch(r){case 0:case"DIRECTION_UNSPECIFIED":return E.DIRECTION_UNSPECIFIED;case 1:case"DIRECTION_ASCENDING":return E.DIRECTION_ASCENDING;case 2:case"DIRECTION_DESCENDING":return E.DIRECTION_DESCENDING;default:return E.UNRECOGNIZED}}(r.direction):E.DIRECTION_UNSPECIFIED,nullsAre:U(r.nullsAre)?function(r){switch(r){case 0:case"NULLS_ARE_UNSPECIFIED":return l.NULLS_ARE_UNSPECIFIED;case 1:case"NULLS_ARE_LARGEST":return l.NULLS_ARE_LARGEST;case 2:case"NULLS_ARE_SMALLEST":return l.NULLS_ARE_SMALLEST;default:return l.UNRECOGNIZED}}(r.nullsAre):l.NULLS_ARE_UNSPECIFIED}),toJSON(r){let e={};return r.direction!==E.DIRECTION_UNSPECIFIED&&(e.direction=function(r){switch(r){case E.DIRECTION_UNSPECIFIED:return"DIRECTION_UNSPECIFIED";case E.DIRECTION_ASCENDING:return"DIRECTION_ASCENDING";case E.DIRECTION_DESCENDING:return"DIRECTION_DESCENDING";case E.UNRECOGNIZED:default:return"UNRECOGNIZED"}}(r.direction)),r.nullsAre!==l.NULLS_ARE_UNSPECIFIED&&(e.nullsAre=function(r){switch(r){case l.NULLS_ARE_UNSPECIFIED:return"NULLS_ARE_UNSPECIFIED";case l.NULLS_ARE_LARGEST:return"NULLS_ARE_LARGEST";case l.NULLS_ARE_SMALLEST:return"NULLS_ARE_SMALLEST";case l.UNRECOGNIZED:default:return"UNRECOGNIZED"}}(r.nullsAre)),e},create:r=>_.fromPartial(null!=r?r:{}),fromPartial(r){var e,t;let o={direction:E.DIRECTION_UNSPECIFIED,nullsAre:l.NULLS_ARE_UNSPECIFIED};return o.direction=null!==(e=r.direction)&&void 0!==e?e:E.DIRECTION_UNSPECIFIED,o.nullsAre=null!==(t=r.nullsAre)&&void 0!==t?t:l.NULLS_ARE_UNSPECIFIED,o}},T={fromJSON:r=>({columnIndex:U(r.columnIndex)?globalThis.Number(r.columnIndex):0,orderType:U(r.orderType)?_.fromJSON(r.orderType):void 0}),toJSON(r){let e={};return 0!==r.columnIndex&&(e.columnIndex=Math.round(r.columnIndex)),void 0!==r.orderType&&(e.orderType=_.toJSON(r.orderType)),e},create:r=>T.fromPartial(null!=r?r:{}),fromPartial(r){var e;let t={columnIndex:0,orderType:void 0};return t.columnIndex=null!==(e=r.columnIndex)&&void 0!==e?e:0,t.orderType=void 0!==r.orderType&&null!==r.orderType?_.fromPartial(r.orderType):void 0,t}};function U(r){return null!=r}}}]);