"use strict";(self.webpackChunkmidjourney_proxy_admin=self.webpackChunkmidjourney_proxy_admin||[]).push([[201],{2891:function(oa,be,l){l.r(be),l.d(be,{default:function(){return sa}});var ye=l(15009),b=l.n(ye),we=l(99289),Q=l.n(we),ke=l(5574),m=l.n(ke),K=l(66927),oe=l(16568),Se=l(86738),L=l(14726),Te=l(82061),u=l(67294),ne=l(80854),e=l(85893),Ae=function(Z){var M=Z.record,i=Z.onSuccess,n=(0,u.useState)(!1),h=m()(n,2),t=h[0],w=h[1],E=(0,u.useState)(!1),z=m()(E,2),D=z[0],F=z[1],v=oe.ZP.useNotification(),N=m()(v,2),k=N[0],$=N[1],T=(0,ne.useIntl)(),R=function(){w(!0)},U=function(){var I=Q()(b()().mark(function A(){var x;return b()().wrap(function(r){for(;;)switch(r.prev=r.next){case 0:return F(!0),r.prev=1,r.next=4,(0,K.tm)(M.id);case 4:x=r.sent,w(!1),x.success?(k.success({message:"success",description:T.formatMessage({id:"pages.account.deleteSuccess"})}),i()):k.error({message:"error",description:x.message}),r.next=12;break;case 9:r.prev=9,r.t0=r.catch(1),console.error(r.t0);case 12:return r.prev=12,F(!1),r.finish(12);case 15:case"end":return r.stop()}},A,null,[[1,9,12,15]])}));return function(){return I.apply(this,arguments)}}(),G=function(){w(!1)};return(0,e.jsxs)(Se.Z,{title:T.formatMessage({id:"pages.account.delete"}),description:T.formatMessage({id:"pages.account.deleteTitle"}),open:t,onConfirm:U,okButtonProps:{loading:D},onCancel:G,children:[$,(0,e.jsx)(L.ZP,{danger:!0,icon:(0,e.jsx)(Te.Z,{}),onClick:R})]})},Oe=Ae,de=l(98165),Re=function(Z){var M=Z.record,i=Z.onSuccess,n=(0,u.useState)(!1),h=m()(n,2),t=h[0],w=h[1],E=(0,u.useState)(!1),z=m()(E,2),D=z[0],F=z[1],v=oe.ZP.useNotification(),N=m()(v,2),k=N[0],$=N[1],T=(0,ne.useIntl)(),R=function(){w(!0)},U=function(){var I=Q()(b()().mark(function A(){var x;return b()().wrap(function(r){for(;;)switch(r.prev=r.next){case 0:return F(!0),r.prev=1,r.next=4,(0,K.Je)(M.id);case 4:x=r.sent,console.log("resss",x),w(!1),x.success?(k.success({message:"success",description:T.formatMessage({id:"pages.account.syncSuccess"})}),i()):(k.error({message:"error",description:x.message}),i()),r.next=13;break;case 10:r.prev=10,r.t0=r.catch(1),console.error(r.t0);case 13:return r.prev=13,F(!1),r.finish(13);case 16:case"end":return r.stop()}},A,null,[[1,10,13,16]])}));return function(){return I.apply(this,arguments)}}(),G=function(){w(!1)};return(0,e.jsxs)(Se.Z,{title:T.formatMessage({id:"pages.account.sync"}),description:T.formatMessage({id:"pages.account.syncTitle"}),open:t,onConfirm:U,okButtonProps:{loading:D},onCancel:G,children:[$,(0,e.jsx)(L.ZP,{icon:(0,e.jsx)(de.Z,{}),onClick:R})]})},Be=Re,a=l(53025),me=l(71230),ee=l(15746),H=l(4393),d=l(55102),P=l(72269),y=l(74656),S=l(37804),re=l(42075),je=l(28248),Ze=l(40056),Ce=l(5785),De=function(Z){var M=Z.form,i=Z.onSubmit,n=(0,ne.useIntl)();(0,u.useEffect)(function(){M.setFieldsValue({userAgent:"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",coreSize:3,queueSize:10,timeoutMinutes:5})});var h=(0,u.useState)([]),t=m()(h,2),w=t[0],E=t[1];(0,u.useEffect)(function(){(0,K.fl)().then(function(I){I.success&&E(I.data)})},[]);var z=(0,u.useState)(!1),D=m()(z,2),F=D[0],v=D[1],N=(0,u.useState)(""),k=m()(N,2),$=k[0],T=k[1],R=function(){v(!0)},U=function(){M.setFieldsValue({subChannels:$.split(`
`)}),v(!1)},G=function(){v(!1)};return(0,e.jsxs)(a.Z,{form:M,labelAlign:"left",layout:"horizontal",labelCol:{span:8},wrapperCol:{span:16},onFinish:i,children:[(0,e.jsxs)(me.Z,{gutter:16,children:[(0,e.jsx)(ee.Z,{span:8,children:(0,e.jsxs)(H.Z,{type:"inner",title:n.formatMessage({id:"pages.account.info"}),children:[(0,e.jsx)(a.Z.Item,{label:n.formatMessage({id:"pages.account.guildId"}),name:"guildId",rules:[{required:!0}],children:(0,e.jsx)(d.Z,{})}),(0,e.jsx)(a.Z.Item,{label:n.formatMessage({id:"pages.account.channelId"}),name:"channelId",rules:[{required:!0}],children:(0,e.jsx)(d.Z,{})}),(0,e.jsx)(a.Z.Item,{label:n.formatMessage({id:"pages.account.userToken"}),name:"userToken",rules:[{required:!0}],children:(0,e.jsx)(d.Z,{})}),(0,e.jsx)(a.Z.Item,{label:n.formatMessage({id:"pages.account.botToken"}),name:"botToken",children:(0,e.jsx)(d.Z,{})}),(0,e.jsx)(a.Z.Item,{label:n.formatMessage({id:"pages.account.mjChannelId"}),name:"privateChannelId",children:(0,e.jsx)(d.Z,{})}),(0,e.jsx)(a.Z.Item,{label:n.formatMessage({id:"pages.account.nijiChannelId"}),name:"nijiBotChannelId",children:(0,e.jsx)(d.Z,{})}),(0,e.jsx)(a.Z.Item,{label:"User Agent",name:"userAgent",children:(0,e.jsx)(d.Z,{})}),(0,e.jsx)(a.Z.Item,{label:n.formatMessage({id:"pages.enable"}),name:"enable",valuePropName:"checked",children:(0,e.jsx)(P.Z,{})}),(0,e.jsx)(a.Z.Item,{label:n.formatMessage({id:"pages.account.remixAutoSubmit"}),name:"remixAutoSubmit",valuePropName:"checked",children:(0,e.jsx)(P.Z,{})}),(0,e.jsx)(a.Z.Item,{label:n.formatMessage({id:"pages.account.mode"}),name:"mode",children:(0,e.jsxs)(y.Z,{allowClear:!0,children:[(0,e.jsx)(y.Z.Option,{value:"RELAX",children:"RELAX"}),(0,e.jsx)(y.Z.Option,{value:"FAST",children:"FAST"}),(0,e.jsx)(y.Z.Option,{value:"TURBO",children:"TURBO"})]})}),(0,e.jsx)(a.Z.Item,{label:n.formatMessage({id:"pages.account.allowModes"}),name:"allowModes",children:(0,e.jsxs)(y.Z,{allowClear:!0,mode:"multiple",children:[(0,e.jsx)(y.Z.Option,{value:"RELAX",children:"RELAX"}),(0,e.jsx)(y.Z.Option,{value:"FAST",children:"FAST"}),(0,e.jsx)(y.Z.Option,{value:"TURBO",children:"TURBO"})]})})]})}),(0,e.jsx)(ee.Z,{span:8,children:(0,e.jsxs)(H.Z,{type:"inner",title:n.formatMessage({id:"pages.account.poolsize"}),children:[(0,e.jsx)(a.Z.Item,{label:n.formatMessage({id:"pages.account.coreSize"}),name:"coreSize",children:(0,e.jsx)(S.Z,{min:1})}),(0,e.jsx)(a.Z.Item,{label:n.formatMessage({id:"pages.account.queueSize"}),name:"queueSize",children:(0,e.jsx)(S.Z,{min:1})}),(0,e.jsx)(a.Z.Item,{label:n.formatMessage({id:"pages.account.maxQueueSize"}),name:"maxQueueSize",children:(0,e.jsx)(S.Z,{min:1})}),(0,e.jsx)(a.Z.Item,{label:n.formatMessage({id:"pages.account.interval"}),name:"interval",children:(0,e.jsx)(S.Z,{min:1.2})}),(0,e.jsx)(a.Z.Item,{label:n.formatMessage({id:"pages.account.intervalAfter"}),children:(0,e.jsx)("div",{style:{display:"flex",flexDirection:"row",alignItems:"center"},children:(0,e.jsxs)(re.Z,{children:[(0,e.jsx)(a.Z.Item,{name:"afterIntervalMin",style:{margin:0},children:(0,e.jsx)(S.Z,{min:1.2,placeholder:"Min"})}),"~",(0,e.jsx)(a.Z.Item,{name:"afterIntervalMax",style:{margin:0},children:(0,e.jsx)(S.Z,{min:1.2,placeholder:"Max"})})]})})}),(0,e.jsx)(a.Z.Item,{label:n.formatMessage({id:"pages.account.weight"}),name:"weight",children:(0,e.jsx)(S.Z,{min:1})}),(0,e.jsx)(a.Z.Item,{label:n.formatMessage({id:"pages.account.enableMj"}),name:"enableMj",children:(0,e.jsx)(P.Z,{})}),(0,e.jsx)(a.Z.Item,{label:n.formatMessage({id:"pages.account.enableNiji"}),name:"enableNiji",children:(0,e.jsx)(P.Z,{})}),(0,e.jsx)(a.Z.Item,{label:n.formatMessage({id:"pages.account.isBlend"}),name:"isBlend",children:(0,e.jsx)(P.Z,{})}),(0,e.jsx)(a.Z.Item,{label:n.formatMessage({id:"pages.account.isDescribe"}),name:"isDescribe",children:(0,e.jsx)(P.Z,{})}),(0,e.jsx)(a.Z.Item,{label:n.formatMessage({id:"pages.account.dayDrawLimit"}),name:"dayDrawLimit",children:(0,e.jsx)(S.Z,{min:-1})})]})}),(0,e.jsx)(ee.Z,{span:8,children:(0,e.jsxs)(H.Z,{type:"inner",title:n.formatMessage({id:"pages.account.otherInfo"}),children:[(0,e.jsx)(a.Z.Item,{label:n.formatMessage({id:"pages.account.permanentInvitationLink"}),name:"permanentInvitationLink",children:(0,e.jsx)(d.Z,{})}),(0,e.jsx)(a.Z.Item,{label:n.formatMessage({id:"pages.account.isVerticalDomain"}),name:"isVerticalDomain",children:(0,e.jsx)(P.Z,{})}),(0,e.jsx)(a.Z.Item,{label:n.formatMessage({id:"pages.account.verticalDomainIds"}),name:"verticalDomainIds",children:(0,e.jsx)(y.Z,{options:w,allowClear:!0,mode:"multiple"})}),(0,e.jsx)(a.Z.Item,{label:n.formatMessage({id:"pages.account.sort"}),name:"sort",children:(0,e.jsx)(S.Z,{})}),(0,e.jsx)(a.Z.Item,{label:n.formatMessage({id:"pages.account.timeoutMinutes"}),name:"timeoutMinutes",children:(0,e.jsx)(S.Z,{min:1,suffix:n.formatMessage({id:"pages.minutes"})})}),(0,e.jsx)(a.Z.Item,{label:n.formatMessage({id:"pages.account.sponsor"}),name:"sponsor",children:(0,e.jsx)(d.Z,{})}),(0,e.jsx)(a.Z.Item,{label:n.formatMessage({id:"pages.account.remark"}),name:"remark",children:(0,e.jsx)(d.Z,{})}),(0,e.jsx)(a.Z.Item,{label:n.formatMessage({id:"pages.account.workTime"}),name:"workTime",children:(0,e.jsx)(d.Z,{placeholder:"09:00-17:00, 18:00-22:00"})}),(0,e.jsx)(a.Z.Item,{label:n.formatMessage({id:"pages.account.fishingTime"}),help:n.formatMessage({id:"pages.account.fishingTimeTips"}),name:"fishingTime",children:(0,e.jsx)(d.Z,{placeholder:"23:30-09:00, 00:00-10:00"})}),(0,e.jsx)(a.Z.Item,{label:n.formatMessage({id:"pages.account.subChannels"}),name:"subChannels",extra:(0,e.jsx)(L.ZP,{type:"primary",style:{marginTop:"10px"},onClick:function(){R()},icon:(0,e.jsx)(Ce.Z,{})}),children:(0,e.jsx)(d.Z.TextArea,{disabled:!0,autoSize:{minRows:1,maxRows:5}})})]})})]}),(0,e.jsxs)(je.Z,{title:n.formatMessage({id:"pages.account.subChannels"}),visible:F,onOk:U,onCancel:G,width:960,children:[(0,e.jsx)("div",{children:(0,e.jsx)(Ze.Z,{message:n.formatMessage({id:"pages.account.subChannelsHelp"}),type:"info",style:{marginBottom:"10px"}})}),(0,e.jsx)(d.Z.TextArea,{placeholder:"https://discord.com/channels/xxx/xxx",autoSize:{minRows:10,maxRows:24},style:{width:"100%"},value:$,onChange:function(A){T(A.target.value)}})]})]})},Fe=De,Ne=function(Z){var M=Z.form,i=Z.onSubmit,n=Z.record,h=(0,ne.useIntl)(),t=oe.ZP.useNotification(),w=m()(t,2),E=w[0],z=w[1],D=(0,u.useState)(),F=m()(D,2),v=F[0],N=F[1],k=(0,u.useState)(!1),$=m()(k,2),T=$[0],R=$[1];(0,u.useEffect)(function(){N(n)},[]);var U=function(){var I=Q()(b()().mark(function A(){var x;return b()().wrap(function(r){for(;;)switch(r.prev=r.next){case 0:return R(!0),r.next=3,(0,K.qG)(n.id);case 3:x=r.sent,R(!1),x.success&&N(x.data);case 6:case"end":return r.stop()}},A)}));return function(){return I.apply(this,arguments)}}(),G=function(){var I=Q()(b()().mark(function A(){var x;return b()().wrap(function(r){for(;;)switch(r.prev=r.next){case 0:return R(!0),r.next=3,(0,K.eD)(n.id);case 3:x=r.sent,R(!1),x.success&&(E.success({message:"success",description:"Success"}),i(v));case 6:case"end":return r.stop()}},A)}));return function(){return I.apply(this,arguments)}}();return(0,e.jsxs)(a.Z,{form:M,labelAlign:"left",layout:"horizontal",labelCol:{span:8},wrapperCol:{span:16},onFinish:i,children:[z,(0,e.jsx)(me.Z,{gutter:16,children:(0,e.jsxs)(ee.Z,{span:24,style:{padding:12},children:[v&&(0,e.jsx)("a",{target:"_blank",rel:"noreferrer",href:v==null?void 0:v.cfUrl,children:(v==null?void 0:v.cfUrl)||"-"}),(0,e.jsx)("br",{}),(0,e.jsxs)(re.Z,{style:{marginTop:12},children:[(0,e.jsx)(L.ZP,{onClick:U,loading:T,type:"default",children:h.formatMessage({id:"pages.account.cfRefresh"})}),(0,e.jsx)(L.ZP,{onClick:G,loading:T,type:"primary",children:h.formatMessage({id:"pages.account.cfok"})})]})]})})]})},Ue=Ne,le=l(66309),ce=l(83062),g=l(26412),Le=l(27484),Pe=l.n(Le),Ee=y.Z.Option,ze=function(Z){var M,i=Z.record,n=Z.onSuccess,h=oe.ZP.useNotification(),t=m()(h,2),w=t[0],E=t[1],z=(0,u.useState)(""),D=m()(z,2),F=D[0],v=D[1],N=(0,u.useState)([]),k=m()(N,2),$=k[0],T=k[1],R=(0,u.useState)([]),U=m()(R,2),G=U[0],I=U[1],A=(0,u.useState)(!1),x=m()(A,2),J=x[0],r=x[1],ge=(0,u.useState)(""),ae=m()(ge,2),ue=ae[0],fe=ae[1],o=(0,ne.useIntl)();(0,u.useEffect)(function(){v(i.version),T(i.buttons),I(i.nijiButtons)},[i]);var pe=function(c,O,_){var X=c?"green":"volcano",W=c?O:_;return(0,e.jsx)(le.Z,{color:X,children:W})},Y=function(c){return!c||c.length<25?c:(0,e.jsx)(ce.Z,{title:c,children:c.substring(0,25)+"..."})},f=function(c){return Pe()(c).format("YYYY-MM-DD HH:mm")},ve=function(){var c;return(c=i.versionSelector)===null||c===void 0?void 0:c.map(function(O){return(0,e.jsxs)(Ee,{value:O.customId,children:[O.emoji," ",O.label]},O.customId)})},he=function(){return $.map(function(c){return(0,e.jsxs)(L.ZP,{ghost:!0,style:{backgroundColor:c.style==3?"#258146":"rgb(131 133 142)"},onClick:function(){xe("MID_JOURNEY",c.customId)},loading:ue=="MID_JOURNEY:"+c.customId,children:[c.emoji," ",c.label]},"MID_JOURNEY:"+c.customId)})},Me=function(){return G.map(function(c){return(0,e.jsxs)(L.ZP,{ghost:!0,style:{backgroundColor:c.style==3?"#258146":"rgb(131 133 142)"},onClick:function(){xe("NIJI_JOURNEY",c.customId)},loading:ue=="NIJI_JOURNEY:"+c.customId,children:[c.emoji," ",c.label]},"NIJI_JOURNEY:"+c.customId)})},Ie=function(){var q=Q()(b()().mark(function c(O){var _;return b()().wrap(function(W){for(;;)switch(W.prev=W.next){case 0:return v(O),r(!0),W.next=4,(0,K.p3)(i.id,O);case 4:_=W.sent,_.success?(r(!1),w.success({message:"success",description:o.formatMessage({id:"pages.account.mjVersionSuccess"})}),n()):(v(i.version),r(!1),w.error({message:"error",description:_.message}));case 6:case"end":return W.stop()}},c)}));return function(O){return q.apply(this,arguments)}}(),xe=function(){var q=Q()(b()().mark(function c(O,_){var X;return b()().wrap(function(se){for(;;)switch(se.prev=se.next){case 0:if(ue===""){se.next=2;break}return se.abrupt("return");case 2:return fe(O+":"+_),se.next=5,(0,K.wO)(i.id,O,_);case 5:X=se.sent,fe(""),X.success?n():w.error({message:"error",description:X.message});case 8:case"end":return se.stop()}},c)}));return function(O,_){return q.apply(this,arguments)}}();return(0,e.jsxs)(e.Fragment,{children:[E,(0,e.jsx)(H.Z,{type:"inner",title:o.formatMessage({id:"pages.account.info"}),style:{margin:"5px"},children:(0,e.jsxs)(g.Z,{column:3,children:[(0,e.jsx)(g.Z.Item,{label:o.formatMessage({id:"pages.account.guildId"}),children:i.guildId}),(0,e.jsx)(g.Z.Item,{label:o.formatMessage({id:"pages.account.channelId"}),children:i.channelId}),(0,e.jsx)(g.Z.Item,{label:o.formatMessage({id:"pages.account.userToken"}),children:Y(i.userToken)}),(0,e.jsx)(g.Z.Item,{label:o.formatMessage({id:"pages.account.botToken"}),children:Y(i.botToken)}),(0,e.jsxs)(g.Z.Item,{label:"User Agent",children:[Y(i.userAgent)," "]}),(0,e.jsx)(g.Z.Item,{label:o.formatMessage({id:"pages.account.remixAutoSubmit"}),children:pe(i.remixAutoSubmit,o.formatMessage({id:"pages.yes"}),o.formatMessage({id:"pages.no"}))}),(0,e.jsx)(g.Z.Item,{label:o.formatMessage({id:"pages.account.mjChannelId"}),children:i.privateChannelId}),(0,e.jsx)(g.Z.Item,{label:o.formatMessage({id:"pages.account.nijiChannelId"}),children:i.nijiBotChannelId})]})}),(0,e.jsx)(H.Z,{type:"inner",title:o.formatMessage({id:"pages.account.basicInfo"}),style:{margin:"5px"},children:(0,e.jsxs)(g.Z,{column:3,children:[(0,e.jsx)(g.Z.Item,{label:o.formatMessage({id:"pages.account.status"}),children:pe(i.enable,o.formatMessage({id:"pages.enable"}),o.formatMessage({id:"pages.disable"}))}),(0,e.jsx)(g.Z.Item,{label:o.formatMessage({id:"pages.account.mjMode"}),children:i.displays.mode}),(0,e.jsx)(g.Z.Item,{label:o.formatMessage({id:"pages.account.nijiMode"}),children:i.displays.nijiMode}),(0,e.jsx)(g.Z.Item,{label:o.formatMessage({id:"pages.account.subscribePlan"}),children:i.displays.subscribePlan}),(0,e.jsx)(g.Z.Item,{label:o.formatMessage({id:"pages.account.billedWay"}),children:i.displays.billedWay}),(0,e.jsx)(g.Z.Item,{label:o.formatMessage({id:"pages.account.renewDate"}),children:i.displays.renewDate}),(0,e.jsx)(g.Z.Item,{label:o.formatMessage({id:"pages.account.fastTimeRemaining"}),children:i.fastTimeRemaining}),(0,e.jsx)(g.Z.Item,{label:o.formatMessage({id:"pages.account.relaxedUsage"}),children:i.relaxedUsage}),(0,e.jsx)(g.Z.Item,{label:o.formatMessage({id:"pages.account.fastUsage"}),children:i.fastUsage}),(0,e.jsx)(g.Z.Item,{label:o.formatMessage({id:"pages.account.turboUsage"}),children:i.turboUsage}),(0,e.jsx)(g.Z.Item,{label:o.formatMessage({id:"pages.account.lifetimeUsage"}),children:i.lifetimeUsage})]})}),(0,e.jsx)(H.Z,{type:"inner",title:o.formatMessage({id:"pages.account.otherInfo"}),style:{margin:"5px"},children:(0,e.jsxs)(g.Z,{column:3,children:[(0,e.jsx)(g.Z.Item,{label:o.formatMessage({id:"pages.account.coreSize"}),children:i.coreSize}),(0,e.jsx)(g.Z.Item,{label:o.formatMessage({id:"pages.account.queueSize"}),children:i.queueSize}),(0,e.jsxs)(g.Z.Item,{label:o.formatMessage({id:"pages.account.timeoutMinutes"}),children:[i.timeoutMinutes," ",o.formatMessage({id:"pages.minutes"})]}),(0,e.jsx)(g.Z.Item,{label:o.formatMessage({id:"pages.account.weight"}),children:i.weight}),(0,e.jsx)(g.Z.Item,{label:o.formatMessage({id:"pages.account.dateCreated"}),children:f(i.dateCreated)}),(0,e.jsx)(g.Z.Item,{label:o.formatMessage({id:"pages.account.remark"}),children:Y(i.remark)}),(0,e.jsx)(g.Z.Item,{label:o.formatMessage({id:"pages.account.disabledReason"}),children:Y(i.disabledReason)})]})}),(0,e.jsxs)(H.Z,{type:"inner",title:o.formatMessage({id:"pages.account.mjSettings"}),style:{margin:"5px"},children:[(0,e.jsx)(y.Z,{style:{width:"35%"},placeholder:(M=i.versionSelector)===null||M===void 0?void 0:M.placeholder,value:F,onChange:Ie,loading:J,children:ve()}),(0,e.jsx)(re.Z,{wrap:!0,style:{marginTop:"5px"},children:he()})]}),(0,e.jsx)(H.Z,{type:"inner",title:o.formatMessage({id:"pages.account.nijiSettings"}),style:{margin:"5px"},children:(0,e.jsx)(re.Z,{wrap:!0,style:{marginTop:"5px"},children:Me()})})]})},$e=ze,Ve=function(Z){var M=Z.form,i=Z.onSubmit,n=Z.record,h=Z.r,t=(0,ne.useIntl)();(0,u.useEffect)(function(){M.setFieldsValue(n)},[]),(0,u.useEffect)(function(){M.setFieldsValue(n)},[h]);var w=(0,u.useState)([]),E=m()(w,2),z=E[0],D=E[1];(0,u.useEffect)(function(){(0,K.fl)().then(function(x){x.success&&D(x.data)})},[]);var F=(0,u.useState)(!1),v=m()(F,2),N=v[0],k=v[1],$=(0,u.useState)(""),T=m()($,2),R=T[0],U=T[1],G=function(){k(!0)},I=function(){n.subChannels=R.split(`
`),M.setFieldsValue({subChannels:R.split(`
`)}),k(!1)},A=function(){k(!1)};return(0,e.jsxs)(a.Z,{form:M,labelAlign:"left",layout:"horizontal",labelCol:{span:8},wrapperCol:{span:16},onFinish:i,children:[(0,e.jsxs)(me.Z,{gutter:16,children:[(0,e.jsx)(ee.Z,{span:8,children:(0,e.jsxs)(H.Z,{type:"inner",title:t.formatMessage({id:"pages.account.info"}),children:[(0,e.jsx)(a.Z.Item,{label:"id",name:"id",hidden:!0,children:(0,e.jsx)(d.Z,{})}),(0,e.jsx)(a.Z.Item,{label:t.formatMessage({id:"pages.account.guildId"}),name:"guildId",rules:[{required:!0}],children:(0,e.jsx)(d.Z,{disabled:!0})}),(0,e.jsx)(a.Z.Item,{label:t.formatMessage({id:"pages.account.channelId"}),name:"channelId",rules:[{required:!0}],children:(0,e.jsx)(d.Z,{disabled:!0})}),(0,e.jsx)(a.Z.Item,{label:t.formatMessage({id:"pages.account.userToken"}),name:"userToken",rules:[{required:!0}],children:(0,e.jsx)(d.Z,{})}),(0,e.jsx)(a.Z.Item,{label:t.formatMessage({id:"pages.account.botToken"}),name:"botToken",children:(0,e.jsx)(d.Z,{})}),(0,e.jsx)(a.Z.Item,{label:t.formatMessage({id:"pages.account.mjChannelId"}),name:"privateChannelId",children:(0,e.jsx)(d.Z,{})}),(0,e.jsx)(a.Z.Item,{label:t.formatMessage({id:"pages.account.nijiChannelId"}),name:"nijiBotChannelId",children:(0,e.jsx)(d.Z,{})}),(0,e.jsx)(a.Z.Item,{label:"User Agent",name:"userAgent",children:(0,e.jsx)(d.Z,{})}),(0,e.jsx)(a.Z.Item,{label:t.formatMessage({id:"pages.enable"}),name:"enable",valuePropName:"checked",children:(0,e.jsx)(P.Z,{})}),(0,e.jsx)(a.Z.Item,{label:t.formatMessage({id:"pages.account.remixAutoSubmit"}),name:"remixAutoSubmit",valuePropName:"checked",children:(0,e.jsx)(P.Z,{})}),(0,e.jsx)(a.Z.Item,{label:t.formatMessage({id:"pages.account.mode"}),name:"mode",children:(0,e.jsxs)(y.Z,{allowClear:!0,children:[(0,e.jsx)(y.Z.Option,{value:"RELAX",children:"RELAX"}),(0,e.jsx)(y.Z.Option,{value:"FAST",children:"FAST"}),(0,e.jsx)(y.Z.Option,{value:"TURBO",children:"TURBO"})]})}),(0,e.jsx)(a.Z.Item,{label:t.formatMessage({id:"pages.account.allowModes"}),name:"allowModes",tooltip:"\u5982\u679C\u7528\u6237\u6307\u5B9A\u6A21\u5F0F\u6216\u6DFB\u52A0\u4E86\u81EA\u5B9A\u4E49\u53C2\u6570\u4F8B\u5982 --fast\uFF0C\u4F46\u662F\u8D26\u53F7\u4E0D\u5141\u8BB8 FAST\uFF0C\u5219\u81EA\u52A8\u79FB\u9664\u6B64\u53C2\u6570",children:(0,e.jsxs)(y.Z,{allowClear:!0,mode:"multiple",children:[(0,e.jsx)(y.Z.Option,{value:"RELAX",children:"RELAX"}),(0,e.jsx)(y.Z.Option,{value:"FAST",children:"FAST"}),(0,e.jsx)(y.Z.Option,{value:"TURBO",children:"TURBO"})]})})]})}),(0,e.jsx)(ee.Z,{span:8,children:(0,e.jsxs)(H.Z,{type:"inner",title:t.formatMessage({id:"pages.account.poolsize"}),children:[(0,e.jsx)(a.Z.Item,{label:t.formatMessage({id:"pages.account.coreSize"}),name:"coreSize",children:(0,e.jsx)(S.Z,{min:1})}),(0,e.jsx)(a.Z.Item,{label:t.formatMessage({id:"pages.account.queueSize"}),name:"queueSize",children:(0,e.jsx)(S.Z,{min:1})}),(0,e.jsx)(a.Z.Item,{label:t.formatMessage({id:"pages.account.maxQueueSize"}),name:"maxQueueSize",children:(0,e.jsx)(S.Z,{min:1})}),(0,e.jsx)(a.Z.Item,{label:t.formatMessage({id:"pages.account.interval"}),name:"interval",children:(0,e.jsx)(S.Z,{min:1.2})}),(0,e.jsx)(a.Z.Item,{label:t.formatMessage({id:"pages.account.intervalAfter"}),children:(0,e.jsx)("div",{style:{display:"flex",flexDirection:"row",alignItems:"center"},children:(0,e.jsxs)(re.Z,{children:[(0,e.jsx)(a.Z.Item,{name:"afterIntervalMin",style:{margin:0},children:(0,e.jsx)(S.Z,{min:1.2,placeholder:"Min"})}),"~",(0,e.jsx)(a.Z.Item,{name:"afterIntervalMax",style:{margin:0},children:(0,e.jsx)(S.Z,{min:1.2,placeholder:"Max"})})]})})}),(0,e.jsx)(a.Z.Item,{label:t.formatMessage({id:"pages.account.weight"}),name:"weight",children:(0,e.jsx)(S.Z,{min:1})}),(0,e.jsx)(a.Z.Item,{label:t.formatMessage({id:"pages.account.enableMj"}),name:"enableMj",children:(0,e.jsx)(P.Z,{})}),(0,e.jsx)(a.Z.Item,{label:t.formatMessage({id:"pages.account.enableNiji"}),name:"enableNiji",children:(0,e.jsx)(P.Z,{})}),(0,e.jsx)(a.Z.Item,{label:t.formatMessage({id:"pages.account.isBlend"}),name:"isBlend",children:(0,e.jsx)(P.Z,{})}),(0,e.jsx)(a.Z.Item,{label:t.formatMessage({id:"pages.account.isDescribe"}),name:"isDescribe",children:(0,e.jsx)(P.Z,{})}),(0,e.jsx)(a.Z.Item,{label:t.formatMessage({id:"pages.account.dayDrawLimit"}),name:"dayDrawLimit",extra:n.dayDrawCount>0&&(0,e.jsxs)("span",{children:[t.formatMessage({id:"pages.account.dayDrawCount"})," ",n.dayDrawCount]}),children:(0,e.jsx)(S.Z,{min:-1})})]})}),(0,e.jsx)(ee.Z,{span:8,children:(0,e.jsxs)(H.Z,{type:"inner",title:t.formatMessage({id:"pages.account.otherInfo"}),children:[(0,e.jsx)(a.Z.Item,{label:t.formatMessage({id:"pages.account.permanentInvitationLink"}),name:"permanentInvitationLink",children:(0,e.jsx)(d.Z,{})}),(0,e.jsx)(a.Z.Item,{label:t.formatMessage({id:"pages.account.isVerticalDomain"}),name:"isVerticalDomain",children:(0,e.jsx)(P.Z,{})}),(0,e.jsx)(a.Z.Item,{label:t.formatMessage({id:"pages.account.verticalDomainIds"}),name:"verticalDomainIds",children:(0,e.jsx)(y.Z,{options:z,allowClear:!0,mode:"multiple"})}),(0,e.jsx)(a.Z.Item,{label:t.formatMessage({id:"pages.account.sort"}),name:"sort",children:(0,e.jsx)(S.Z,{})}),(0,e.jsx)(a.Z.Item,{label:t.formatMessage({id:"pages.account.timeoutMinutes"}),name:"timeoutMinutes",children:(0,e.jsx)(S.Z,{min:1,suffix:t.formatMessage({id:"pages.minutes"})})}),(0,e.jsx)(a.Z.Item,{label:t.formatMessage({id:"pages.account.sponsor"}),name:"sponsor",children:(0,e.jsx)(d.Z,{})}),(0,e.jsx)(a.Z.Item,{label:t.formatMessage({id:"pages.account.remark"}),name:"remark",children:(0,e.jsx)(d.Z,{})}),(0,e.jsx)(a.Z.Item,{label:t.formatMessage({id:"pages.account.workTime"}),name:"workTime",children:(0,e.jsx)(d.Z,{placeholder:"09:00-17:00, 18:00-22:00"})}),(0,e.jsx)(a.Z.Item,{label:t.formatMessage({id:"pages.account.fishingTime"}),help:t.formatMessage({id:"pages.account.fishingTimeTips"}),name:"fishingTime",children:(0,e.jsx)(d.Z,{placeholder:"23:30-09:00, 00:00-10:00"})}),(0,e.jsx)(a.Z.Item,{label:t.formatMessage({id:"pages.account.subChannels"}),name:"subChannels",extra:(0,e.jsx)(L.ZP,{type:"primary",style:{marginTop:"10px"},onClick:function(){U(M.getFieldValue("subChannels").join(`
`)),G()},icon:(0,e.jsx)(Ce.Z,{})}),children:(0,e.jsx)(d.Z.TextArea,{disabled:!0,autoSize:{minRows:1,maxRows:5}})})]})})]}),(0,e.jsxs)(je.Z,{title:t.formatMessage({id:"pages.account.subChannels"}),visible:N,onOk:I,onCancel:A,width:960,children:[(0,e.jsx)("div",{children:(0,e.jsx)(Ze.Z,{message:t.formatMessage({id:"pages.account.subChannelsHelp"}),type:"info",style:{marginBottom:"10px"}})}),(0,e.jsx)(d.Z.TextArea,{placeholder:"https://discord.com/channels/xxx/xxx",autoSize:{minRows:10,maxRows:24},style:{width:"100%"},value:R,onChange:function(J){U(J.target.value)}})]}),(0,e.jsx)(Ze.Z,{message:t.formatMessage({id:"pages.account.updateNotice"}),type:"warning",style:{marginTop:"10px"}})]})},He=Ve,Je=function(Z){var M=Z.form,i=Z.onSubmit,n=Z.record,h=(0,ne.useIntl)();return(0,u.useEffect)(function(){M.setFieldsValue(n)}),(0,e.jsx)(a.Z,{form:M,labelAlign:"left",layout:"horizontal",labelCol:{span:8},wrapperCol:{span:16},onFinish:i,children:(0,e.jsxs)(me.Z,{gutter:16,children:[(0,e.jsx)(ee.Z,{span:12,children:(0,e.jsxs)(H.Z,{type:"inner",title:h.formatMessage({id:"pages.account.info"}),children:[(0,e.jsx)(a.Z.Item,{label:"id",name:"id",hidden:!0,children:(0,e.jsx)(d.Z,{})}),(0,e.jsx)(a.Z.Item,{label:h.formatMessage({id:"pages.account.mjChannelId"}),name:"privateChannelId",children:(0,e.jsx)(d.Z,{})}),(0,e.jsx)(a.Z.Item,{label:h.formatMessage({id:"pages.account.nijiChannelId"}),name:"nijiBotChannelId",children:(0,e.jsx)(d.Z,{})}),(0,e.jsx)(a.Z.Item,{label:h.formatMessage({id:"pages.account.remixAutoSubmit"}),name:"remixAutoSubmit",valuePropName:"checked",children:(0,e.jsx)(P.Z,{})})]})}),(0,e.jsx)(ee.Z,{span:12,children:(0,e.jsxs)(H.Z,{type:"inner",title:h.formatMessage({id:"pages.account.otherInfo"}),children:[(0,e.jsx)(a.Z.Item,{label:h.formatMessage({id:"pages.account.timeoutMinutes"}),name:"timeoutMinutes",children:(0,e.jsx)(S.Z,{min:1,suffix:h.formatMessage({id:"pages.minutes"})})}),(0,e.jsx)(a.Z.Item,{label:h.formatMessage({id:"pages.account.weight"}),name:"weight",children:(0,e.jsx)(S.Z,{min:1})}),(0,e.jsx)(a.Z.Item,{label:h.formatMessage({id:"pages.account.remark"}),name:"remark",children:(0,e.jsx)(d.Z,{})}),(0,e.jsx)(a.Z.Item,{label:h.formatMessage({id:"pages.account.sponsor"}),name:"sponsor",children:(0,e.jsx)(d.Z,{})}),(0,e.jsx)(a.Z.Item,{label:h.formatMessage({id:"pages.account.sort"}),name:"sort",children:(0,e.jsx)(S.Z,{})})]})})]})})},Ye=Je,Xe=l(94149),We=l(66212),Ge=l(3355),Qe=l(2830),Ke=l(47389),qe=l(88360),_e=l(90930),ea=l(72051),aa=function(){var Z=(0,u.useState)(!1),M=m()(Z,2),i=M[0],n=M[1],h=(0,u.useState)(!1),t=m()(h,2),w=t[0],E=t[1],z=(0,u.useState)((0,e.jsx)(e.Fragment,{})),D=m()(z,2),F=D[0],v=D[1],N=(0,u.useState)(""),k=m()(N,2),$=k[0],T=k[1],R=(0,u.useState)(1e3),U=m()(R,2),G=U[0],I=U[1],A=a.Z.useForm(),x=m()(A,1),J=x[0],r=oe.ZP.useNotification(),ge=m()(r,2),ae=ge[0],ue=ge[1],fe=(0,u.useState)(!1),o=m()(fe,2),pe=o[0],Y=o[1],f=(0,ne.useIntl)(),ve=(0,u.useState)([]),he=m()(ve,2),Me=he[0],Ie=he[1],xe=(0,u.useState)(!1),q=m()(xe,2),c=q[0],O=q[1],_=function(){var C=Q()(b()().mark(function j(){var s;return b()().wrap(function(V){for(;;)switch(V.prev=V.next){case 0:return O(!0),V.next=3,(0,K.Ed)();case 3:s=V.sent,Ie(s),O(!1);case 6:case"end":return V.stop()}},j)}));return function(){return C.apply(this,arguments)}}(),X=function(){v((0,e.jsx)(e.Fragment,{})),n(!1),E(!1)},W=function(j,s,p){J.resetFields(),T(j),v(s),I(p),n(!0)},se=(0,e.jsxs)(e.Fragment,{children:[(0,e.jsx)(L.ZP,{onClick:X,children:f.formatMessage({id:"pages.cancel"})},"back"),(0,e.jsx)(L.ZP,{type:"primary",loading:pe,onClick:function(){return J.submit()},children:f.formatMessage({id:"pages.submit"})},"submit")]}),te=function(){X(),_()},na=function(){var C=Q()(b()().mark(function j(s){var p;return b()().wrap(function(B){for(;;)switch(B.prev=B.next){case 0:return s.subChannels||(s.subChannels=[]),typeof s.subChannels=="string"&&(s.subChannels=s.subChannels.split(`
`)),Y(!0),B.next=5,(0,K.o1)(s);case 5:p=B.sent,Y(!1),console.log("res",p),p.success?(ae.success({message:"success",description:p.message}),X(),te()):ae.error({message:"error",description:p.message});case 9:case"end":return B.stop()}},j)}));return function(s){return C.apply(this,arguments)}}(),ta=function(){var C=Q()(b()().mark(function j(s){var p;return b()().wrap(function(B){for(;;)switch(B.prev=B.next){case 0:return s.subChannels||(s.subChannels=[]),typeof s.subChannels=="string"&&(s.subChannels=s.subChannels.split(`
`)),Y(!0),B.next=5,(0,K.MS)(s.id,s);case 5:p=B.sent,p.success?(ae.success({message:"success",description:p.message}),X(),te(),Y(!1)):ae.error({message:"error",description:p.message});case 7:case"end":return B.stop()}},j)}));return function(s){return C.apply(this,arguments)}}(),ia=function(){var C=Q()(b()().mark(function j(s){var p;return b()().wrap(function(B){for(;;)switch(B.prev=B.next){case 0:return s.subChannels||(s.subChannels=[]),typeof s.subChannels=="string"&&(s.subChannels=s.subChannels.split(`
`)),Y(!0),B.next=5,(0,K.Vx)(s.id,s);case 5:p=B.sent,p.success?ae.success({message:"success",description:p.message}):ae.error({message:"error",description:p.message}),X(),te(),Y(!1);case 10:case"end":return B.stop()}},j)}));return function(s){return C.apply(this,arguments)}}(),ra=function(){var C=Q()(b()().mark(function j(){return b()().wrap(function(p){for(;;)switch(p.prev=p.next){case 0:Y(!0),X(),te(),Y(!1);case 4:case"end":return p.stop()}},j)}));return function(){return C.apply(this,arguments)}}(),la=[{title:f.formatMessage({id:"pages.account.guildId"}),dataIndex:"guildId",width:200,align:"center",render:function(j,s){return(0,e.jsx)("a",{onClick:function(){E(!0),W(f.formatMessage({id:"pages.account.info"})+" - "+s.id,(0,e.jsx)($e,{record:s,onSuccess:te}),1100)},children:j})}},{title:f.formatMessage({id:"pages.account.channelId"}),dataIndex:"channelId",align:"center",width:200},{title:"".concat(f.formatMessage({id:"pages.account.status"})),dataIndex:"enable",width:200,align:"center",request:function(){var C=Q()(b()().mark(function s(){return b()().wrap(function(V){for(;;)switch(V.prev=V.next){case 0:return V.abrupt("return",[{label:f.formatMessage({id:"pages.enable"}),value:"true"},{label:f.formatMessage({id:"pages.disable"}),value:"false"}]);case 1:case"end":return V.stop()}},s)}));function j(){return C.apply(this,arguments)}return j}(),render:function(j,s){var p=j?"green":"volcano",V=j?f.formatMessage({id:"pages.enable"}):f.formatMessage({id:"pages.disable"});return(0,e.jsxs)(e.Fragment,{children:[(0,e.jsx)(le.Z,{color:p,children:V}),s.lock&&(0,e.jsx)(le.Z,{icon:(0,e.jsx)(Xe.Z,{}),color:"warning",children:(0,e.jsx)(ce.Z,{title:f.formatMessage({id:"pages.account.lockmsg"}),children:f.formatMessage({id:"pages.account.lock"})})}),j&&!s.running&&!s.lock&&(0,e.jsx)(le.Z,{icon:(0,e.jsx)(de.Z,{}),color:"error",children:f.formatMessage({id:"pages.account.notRunning"})}),(s==null?void 0:s.runningCount)>0&&(0,e.jsx)(le.Z,{icon:(0,e.jsx)(de.Z,{spin:!0}),color:"cyan",children:(0,e.jsx)(ce.Z,{title:f.formatMessage({id:"pages.account.runningCount"})+" "+s.runningCount,children:(s==null?void 0:s.runningCount)||0})}),(s==null?void 0:s.queueCount)>0&&(0,e.jsx)(le.Z,{icon:(0,e.jsx)(We.Z,{}),color:"processing",children:(0,e.jsx)(ce.Z,{title:f.formatMessage({id:"pages.account.queueCount"})+" "+s.queueCount,children:s.queueCount||0})})]})}},{title:f.formatMessage({id:"pages.account.fastTimeRemaining"}),dataIndex:"fastTimeRemaining",ellipsis:!0,width:200,hideInSearch:!0,align:"center"},{title:f.formatMessage({id:"pages.account.renewDate"}),dataIndex:"renewDate",align:"center",width:160,hideInSearch:!0,render:function(j,s){return s.displays.renewDate}},{title:f.formatMessage({id:"pages.account.sponsor"}),dataIndex:"sponsor",ellipsis:!0,width:100,render:function(j,s){return(0,e.jsx)("div",{dangerouslySetInnerHTML:{__html:s.sponsor||"-"}})}},{title:f.formatMessage({id:"pages.account.remark"}),dataIndex:"remark",ellipsis:!0,width:150},{title:f.formatMessage({id:"pages.account.disabledReason"}),dataIndex:"disabledReason",ellipsis:!0,width:150,hideInSearch:!0},{title:f.formatMessage({id:"pages.operation"}),dataIndex:"operation",width:220,key:"operation",fixed:"right",align:"center",hideInSearch:!0,render:function(j,s){return(0,e.jsxs)(re.Z,{children:[s.lock&&(0,e.jsx)(L.ZP,{icon:(0,e.jsx)(Ge.Z,{}),type:"dashed",onClick:function(){return W(f.formatMessage({id:"pages.account.cfmodal"}),(0,e.jsx)(Ue,{form:J,record:s,onSubmit:ra}),1e3)}},"Lock"),(0,e.jsx)(Be,{record:s,onSuccess:te}),(0,e.jsx)(ce.Z,{title:f.formatMessage({id:"pages.account.updateAndReconnect"}),children:(0,e.jsx)(L.ZP,{type:"primary",icon:(0,e.jsx)(Qe.Z,{}),onClick:function(){W(f.formatMessage({id:"pages.account.updateAndReconnect"}),(0,e.jsx)(He,{r:Math.random(),form:J,record:s,onSubmit:ta}),1600)}},"EditAndReconnect")}),(0,e.jsx)(L.ZP,{icon:(0,e.jsx)(Ke.Z,{}),onClick:function(){return W(f.formatMessage({id:"pages.account.update"}),(0,e.jsx)(Ye,{form:J,record:s,onSubmit:ia}),1e3)}},"Update"),(0,e.jsx)(Oe,{record:s,onSuccess:te})]})}}];return(0,u.useEffect)(function(){_()},[]),(0,e.jsxs)(_e._z,{children:[ue,(0,e.jsxs)(H.Z,{children:[(0,e.jsxs)("div",{style:{display:"flex",justifyContent:"flex-end",marginBottom:16},children:[(0,e.jsx)(L.ZP,{type:"primary",icon:(0,e.jsx)(qe.Z,{}),onClick:function(){W(f.formatMessage({id:"pages.account.add"}),(0,e.jsx)(Fe,{form:J,onSubmit:na}),1600)},children:f.formatMessage({id:"pages.add"})},"primary"),(0,e.jsx)(L.ZP,{type:"default",style:{marginLeft:8},icon:(0,e.jsx)(de.Z,{}),onClick:function(){te()}})]}),(0,e.jsx)(ea.Z,{scroll:{x:1e3},rowKey:"id",columns:la,dataSource:Me,loading:c,pagination:!1})]}),(0,e.jsx)(je.Z,{title:$,open:i,onCancel:X,footer:w?null:se,width:G,children:F})]})},sa=aa}}]);