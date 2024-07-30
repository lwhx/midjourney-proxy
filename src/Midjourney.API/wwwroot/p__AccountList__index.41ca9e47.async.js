"use strict";(self.webpackChunkmidjourney_proxy_admin=self.webpackChunkmidjourney_proxy_admin||[]).push([[201],{94149:function(ke,me,s){s.d(me,{Z:function(){return re}});var ge=s(1413),x=s(67294),Ze={icon:{tag:"svg",attrs:{viewBox:"64 64 896 896",focusable:"false"},children:[{tag:"path",attrs:{d:"M832 464h-68V240c0-70.7-57.3-128-128-128H388c-70.7 0-128 57.3-128 128v224h-68c-17.7 0-32 14.3-32 32v384c0 17.7 14.3 32 32 32h640c17.7 0 32-14.3 32-32V496c0-17.7-14.3-32-32-32zM332 240c0-30.9 25.1-56 56-56h248c30.9 0 56 25.1 56 56v224H332V240zm460 600H232V536h560v304zM484 701v53c0 4.4 3.6 8 8 8h40c4.4 0 8-3.6 8-8v-53a48.01 48.01 0 10-56 0z"}}]},name:"lock",theme:"outlined"},z=Ze,Me=s(91146),h=function(O,Se){return x.createElement(Me.Z,(0,ge.Z)((0,ge.Z)({},O),{},{ref:Se,icon:z}))},P=x.forwardRef(h),re=P},63667:function(ke,me,s){s.r(me),s.d(me,{default:function(){return xa}});var ge=s(15009),x=s.n(ge),Ze=s(99289),z=s.n(Ze),Me=s(5574),h=s.n(Me),P=s(66927),re=s(16568),Ie=s(86738),O=s(14726),Se=s(82061),u=s(67294),ae=s(80854),e=s(85893),Re=function(d){var g=d.record,a=d.onSuccess,l=(0,u.useState)(!1),r=h()(l,2),V=r[0],C=r[1],_=(0,u.useState)(!1),H=h()(_,2),J=H[0],Y=H[1],D=re.ZP.useNotification(),W=h()(D,2),B=W[0],ne=W[1],N=(0,ae.useIntl)(),se=function(){C(!0)},Q=function(){var L=z()(x()().mark(function X(){var y;return x()().wrap(function(c){for(;;)switch(c.prev=c.next){case 0:return Y(!0),c.prev=1,c.next=4,(0,P.tm)(g.id);case 4:y=c.sent,C(!1),y.success?(B.success({message:"success",description:N.formatMessage({id:"pages.account.deleteSuccess"})}),a()):B.error({message:"error",description:y.message}),c.next=12;break;case 9:c.prev=9,c.t0=c.catch(1),console.error(c.t0);case 12:return c.prev=12,Y(!1),c.finish(12);case 15:case"end":return c.stop()}},X,null,[[1,9,12,15]])}));return function(){return L.apply(this,arguments)}}(),te=function(){C(!1)};return(0,e.jsxs)(Ie.Z,{title:N.formatMessage({id:"pages.account.delete"}),description:N.formatMessage({id:"pages.account.deleteTitle"}),open:V,onConfirm:Q,okButtonProps:{loading:J},onCancel:te,children:[ne,(0,e.jsx)(O.ZP,{danger:!0,icon:(0,e.jsx)(Se.Z,{}),onClick:se})]})},Te=Re,F=s(1413),we={icon:{tag:"svg",attrs:{viewBox:"64 64 896 896",focusable:"false"},children:[{tag:"path",attrs:{d:"M168 504.2c1-43.7 10-86.1 26.9-126 17.3-41 42.1-77.7 73.7-109.4S337 212.3 378 195c42.4-17.9 87.4-27 133.9-27s91.5 9.1 133.8 27A341.5 341.5 0 01755 268.8c9.9 9.9 19.2 20.4 27.8 31.4l-60.2 47a8 8 0 003 14.1l175.7 43c5 1.2 9.9-2.6 9.9-7.7l.8-180.9c0-6.7-7.7-10.5-12.9-6.3l-56.4 44.1C765.8 155.1 646.2 92 511.8 92 282.7 92 96.3 275.6 92 503.8a8 8 0 008 8.2h60c4.4 0 7.9-3.5 8-7.8zm756 7.8h-60c-4.4 0-7.9 3.5-8 7.8-1 43.7-10 86.1-26.9 126-17.3 41-42.1 77.8-73.7 109.4A342.45 342.45 0 01512.1 856a342.24 342.24 0 01-243.2-100.8c-9.9-9.9-19.2-20.4-27.8-31.4l60.2-47a8 8 0 00-3-14.1l-175.7-43c-5-1.2-9.9 2.6-9.9 7.7l-.7 181c0 6.7 7.7 10.5 12.9 6.3l56.4-44.1C258.2 868.9 377.8 932 512.2 932c229.2 0 415.5-183.7 419.8-411.8a8 8 0 00-8-8.2z"}}]},name:"sync",theme:"outlined"},ze=we,le=s(91146),Ue=function(d,g){return u.createElement(le.Z,(0,F.Z)((0,F.Z)({},d),{},{ref:g,icon:ze}))},Be=u.forwardRef(Ue),fe=Be,Ne=function(d){var g=d.record,a=d.onSuccess,l=(0,u.useState)(!1),r=h()(l,2),V=r[0],C=r[1],_=(0,u.useState)(!1),H=h()(_,2),J=H[0],Y=H[1],D=re.ZP.useNotification(),W=h()(D,2),B=W[0],ne=W[1],N=(0,ae.useIntl)(),se=function(){C(!0)},Q=function(){var L=z()(x()().mark(function X(){var y;return x()().wrap(function(c){for(;;)switch(c.prev=c.next){case 0:return Y(!0),c.prev=1,c.next=4,(0,P.Je)(g.id);case 4:y=c.sent,console.log("resss",y),C(!1),y.success?(B.success({message:"success",description:N.formatMessage({id:"pages.account.syncSuccess"})}),a()):(B.error({message:"error",description:y.message}),a()),c.next=13;break;case 10:c.prev=10,c.t0=c.catch(1),console.error(c.t0);case 13:return c.prev=13,Y(!1),c.finish(13);case 16:case"end":return c.stop()}},X,null,[[1,10,13,16]])}));return function(){return L.apply(this,arguments)}}(),te=function(){C(!1)};return(0,e.jsxs)(Ie.Z,{title:N.formatMessage({id:"pages.account.sync"}),description:N.formatMessage({id:"pages.account.syncTitle"}),open:V,onConfirm:Q,okButtonProps:{loading:J},onCancel:te,children:[ne,(0,e.jsx)(O.ZP,{icon:(0,e.jsx)(fe,{}),onClick:se})]})},Le=Ne,n=s(53025),be=s(71230),ie=s(15746),A=s(4393),p=s(55102),ce=s(72269),k=s(37804),$=s(74656),Ee=function(d){var g=d.form,a=d.onSubmit,l=(0,ae.useIntl)();return(0,u.useEffect)(function(){g.setFieldsValue({userAgent:"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",coreSize:3,queueSize:10,timeoutMinutes:5})}),(0,e.jsx)(n.Z,{form:g,labelAlign:"left",layout:"horizontal",labelCol:{span:8},wrapperCol:{span:16},onFinish:a,children:(0,e.jsxs)(be.Z,{gutter:16,children:[(0,e.jsx)(ie.Z,{span:12,children:(0,e.jsxs)(A.Z,{type:"inner",title:l.formatMessage({id:"pages.account.info"}),children:[(0,e.jsx)(n.Z.Item,{label:l.formatMessage({id:"pages.account.guildId"}),name:"guildId",rules:[{required:!0}],children:(0,e.jsx)(p.Z,{})}),(0,e.jsx)(n.Z.Item,{label:l.formatMessage({id:"pages.account.channelId"}),name:"channelId",rules:[{required:!0}],children:(0,e.jsx)(p.Z,{})}),(0,e.jsx)(n.Z.Item,{label:l.formatMessage({id:"pages.account.userToken"}),name:"userToken",rules:[{required:!0}],children:(0,e.jsx)(p.Z,{})}),(0,e.jsx)(n.Z.Item,{label:l.formatMessage({id:"pages.account.botToken"}),name:"botToken",rules:[{required:!0}],children:(0,e.jsx)(p.Z,{})}),(0,e.jsx)(n.Z.Item,{label:l.formatMessage({id:"pages.account.mjChannelId"}),name:"privateChannelId",children:(0,e.jsx)(p.Z,{})}),(0,e.jsx)(n.Z.Item,{label:l.formatMessage({id:"pages.account.nijiChannelId"}),name:"nijiBotChannelId",children:(0,e.jsx)(p.Z,{})}),(0,e.jsx)(n.Z.Item,{label:"User Agent",name:"userAgent",children:(0,e.jsx)(p.Z,{})}),(0,e.jsx)(n.Z.Item,{label:l.formatMessage({id:"pages.enable"}),name:"enable",valuePropName:"checked",children:(0,e.jsx)(ce.Z,{})}),(0,e.jsx)(n.Z.Item,{label:l.formatMessage({id:"pages.account.remixAutoSubmit"}),name:"remixAutoSubmit",valuePropName:"checked",children:(0,e.jsx)(ce.Z,{})})]})}),(0,e.jsx)(ie.Z,{span:12,children:(0,e.jsxs)(A.Z,{type:"inner",title:l.formatMessage({id:"pages.account.otherInfo"}),children:[(0,e.jsx)(n.Z.Item,{label:l.formatMessage({id:"pages.account.coreSize"}),name:"coreSize",children:(0,e.jsx)(k.Z,{min:1})}),(0,e.jsx)(n.Z.Item,{label:l.formatMessage({id:"pages.account.queueSize"}),name:"queueSize",children:(0,e.jsx)(k.Z,{min:1})}),(0,e.jsx)(n.Z.Item,{label:l.formatMessage({id:"pages.account.maxQueueSize"}),name:"maxQueueSize",children:(0,e.jsx)(k.Z,{min:1})}),(0,e.jsx)(n.Z.Item,{label:l.formatMessage({id:"pages.account.mode"}),name:"mode",children:(0,e.jsxs)($.Z,{allowClear:!0,children:[(0,e.jsx)($.Z.Option,{value:"RELAX",children:"RELAX"}),(0,e.jsx)($.Z.Option,{value:"FAST",children:"FAST"}),(0,e.jsx)($.Z.Option,{value:"TURBO",children:"TURBO"})]})}),(0,e.jsx)(n.Z.Item,{label:l.formatMessage({id:"pages.account.timeoutMinutes"}),name:"timeoutMinutes",children:(0,e.jsx)(k.Z,{min:1,suffix:l.formatMessage({id:"pages.minutes"})})}),(0,e.jsx)(n.Z.Item,{label:l.formatMessage({id:"pages.account.weight"}),name:"weight",children:(0,e.jsx)(k.Z,{min:1})}),(0,e.jsx)(n.Z.Item,{label:l.formatMessage({id:"pages.account.remark"}),name:"remark",children:(0,e.jsx)(p.Z,{})}),(0,e.jsx)(n.Z.Item,{label:l.formatMessage({id:"pages.account.sponsor"}),name:"sponsor",children:(0,e.jsx)(p.Z,{})})]})})]})})},Pe=Ee,oe=s(66309),ue=s(83062),m=s(26412),Ce=s(42075),Fe=s(27484),$e=s.n(Fe),De=$.Z.Option,Ve=function(d){var g,a=d.record,l=d.onSuccess,r=re.ZP.useNotification(),V=h()(r,2),C=V[0],_=V[1],H=(0,u.useState)(""),J=h()(H,2),Y=J[0],D=J[1],W=(0,u.useState)([]),B=h()(W,2),ne=B[0],N=B[1],se=(0,u.useState)([]),Q=h()(se,2),te=Q[0],L=Q[1],X=(0,u.useState)(!1),y=h()(X,2),G=y[0],c=y[1],he=(0,u.useState)(""),K=h()(he,2),de=K[0],pe=K[1],i=(0,ae.useIntl)();(0,u.useEffect)(function(){D(a.version),N(a.buttons),L(a.nijiButtons)},[a]);var ve=function(o,M,w){var E=o?"green":"volcano",S=o?M:w;return(0,e.jsx)(oe.Z,{color:E,children:S})},R=function(o){return!o||o.length<25?o:(0,e.jsx)(ue.Z,{title:o,children:o.substring(0,25)+"..."})},f=function(o){return $e()(o).format("YYYY-MM-DD HH:mm")},ye=function(){var o;return(o=a.versionSelector)===null||o===void 0?void 0:o.map(function(M){return(0,e.jsxs)(De,{value:M.customId,children:[M.emoji," ",M.label]},M.customId)})},je=function(){return ne.map(function(o){return(0,e.jsxs)(O.ZP,{ghost:!0,style:{backgroundColor:o.style==3?"#258146":"rgb(131 133 142)"},onClick:function(){xe("MID_JOURNEY",o.customId)},loading:de=="MID_JOURNEY:"+o.customId,children:[o.emoji," ",o.label]},"MID_JOURNEY:"+o.customId)})},Oe=function(){return te.map(function(o){return(0,e.jsxs)(O.ZP,{ghost:!0,style:{backgroundColor:o.style==3?"#258146":"rgb(131 133 142)"},onClick:function(){xe("NIJI_JOURNEY",o.customId)},loading:de=="NIJI_JOURNEY:"+o.customId,children:[o.emoji," ",o.label]},"NIJI_JOURNEY:"+o.customId)})},Ae=function(){var T=z()(x()().mark(function o(M){var w;return x()().wrap(function(S){for(;;)switch(S.prev=S.next){case 0:return D(M),c(!0),S.next=4,(0,P.p3)(a.id,M);case 4:w=S.sent,w.success?(c(!1),C.success({message:"success",description:i.formatMessage({id:"pages.account.mjVersionSuccess"})}),l()):(D(a.version),c(!1),C.error({message:"error",description:w.message}));case 6:case"end":return S.stop()}},o)}));return function(M){return T.apply(this,arguments)}}(),xe=function(){var T=z()(x()().mark(function o(M,w){var E;return x()().wrap(function(q){for(;;)switch(q.prev=q.next){case 0:if(de===""){q.next=2;break}return q.abrupt("return");case 2:return pe(M+":"+w),q.next=5,(0,P.wO)(a.id,M,w);case 5:E=q.sent,pe(""),E.success?l():C.error({message:"error",description:E.message});case 8:case"end":return q.stop()}},o)}));return function(M,w){return T.apply(this,arguments)}}();return(0,e.jsxs)(e.Fragment,{children:[_,(0,e.jsx)(A.Z,{type:"inner",title:i.formatMessage({id:"pages.account.info"}),style:{margin:"5px"},children:(0,e.jsxs)(m.Z,{column:3,children:[(0,e.jsx)(m.Z.Item,{label:i.formatMessage({id:"pages.account.guildId"}),children:a.guildId}),(0,e.jsx)(m.Z.Item,{label:i.formatMessage({id:"pages.account.channelId"}),children:a.channelId}),(0,e.jsx)(m.Z.Item,{label:i.formatMessage({id:"pages.account.userToken"}),children:R(a.userToken)}),(0,e.jsx)(m.Z.Item,{label:i.formatMessage({id:"pages.account.botToken"}),children:R(a.botToken)}),(0,e.jsxs)(m.Z.Item,{label:"User Agent",children:[R(a.userAgent)," "]}),(0,e.jsx)(m.Z.Item,{label:i.formatMessage({id:"pages.account.remixAutoSubmit"}),children:ve(a.remixAutoSubmit,i.formatMessage({id:"pages.yes"}),i.formatMessage({id:"pages.no"}))}),(0,e.jsx)(m.Z.Item,{label:i.formatMessage({id:"pages.account.mjChannelId"}),children:a.privateChannelId}),(0,e.jsx)(m.Z.Item,{label:i.formatMessage({id:"pages.account.nijiChannelId"}),children:a.nijiBotChannelId})]})}),(0,e.jsx)(A.Z,{type:"inner",title:i.formatMessage({id:"pages.account.basicInfo"}),style:{margin:"5px"},children:(0,e.jsxs)(m.Z,{column:3,children:[(0,e.jsx)(m.Z.Item,{label:i.formatMessage({id:"pages.account.status"}),children:ve(a.enable,i.formatMessage({id:"pages.enable"}),i.formatMessage({id:"pages.disable"}))}),(0,e.jsx)(m.Z.Item,{label:i.formatMessage({id:"pages.account.mjMode"}),children:a.displays.mode}),(0,e.jsx)(m.Z.Item,{label:i.formatMessage({id:"pages.account.nijiMode"}),children:a.displays.nijiMode}),(0,e.jsx)(m.Z.Item,{label:i.formatMessage({id:"pages.account.subscribePlan"}),children:a.displays.subscribePlan}),(0,e.jsx)(m.Z.Item,{label:i.formatMessage({id:"pages.account.billedWay"}),children:a.displays.billedWay}),(0,e.jsx)(m.Z.Item,{label:i.formatMessage({id:"pages.account.renewDate"}),children:a.displays.renewDate}),(0,e.jsx)(m.Z.Item,{label:i.formatMessage({id:"pages.account.fastTimeRemaining"}),children:a.fastTimeRemaining}),(0,e.jsx)(m.Z.Item,{label:i.formatMessage({id:"pages.account.relaxedUsage"}),children:a.relaxedUsage}),(0,e.jsx)(m.Z.Item,{label:i.formatMessage({id:"pages.account.fastUsage"}),children:a.fastUsage}),(0,e.jsx)(m.Z.Item,{label:i.formatMessage({id:"pages.account.turboUsage"}),children:a.turboUsage}),(0,e.jsx)(m.Z.Item,{label:i.formatMessage({id:"pages.account.lifetimeUsage"}),children:a.lifetimeUsage})]})}),(0,e.jsx)(A.Z,{type:"inner",title:i.formatMessage({id:"pages.account.otherInfo"}),style:{margin:"5px"},children:(0,e.jsxs)(m.Z,{column:3,children:[(0,e.jsx)(m.Z.Item,{label:i.formatMessage({id:"pages.account.coreSize"}),children:a.coreSize}),(0,e.jsx)(m.Z.Item,{label:i.formatMessage({id:"pages.account.queueSize"}),children:a.queueSize}),(0,e.jsxs)(m.Z.Item,{label:i.formatMessage({id:"pages.account.timeoutMinutes"}),children:[a.timeoutMinutes," ",i.formatMessage({id:"pages.minutes"})]}),(0,e.jsx)(m.Z.Item,{label:i.formatMessage({id:"pages.account.weight"}),children:a.weight}),(0,e.jsx)(m.Z.Item,{label:i.formatMessage({id:"pages.account.dateCreated"}),children:f(a.dateCreated)}),(0,e.jsx)(m.Z.Item,{label:i.formatMessage({id:"pages.account.remark"}),children:R(a.remark)}),(0,e.jsx)(m.Z.Item,{label:i.formatMessage({id:"pages.account.disabledReason"}),children:R(a.disabledReason)})]})}),(0,e.jsxs)(A.Z,{type:"inner",title:i.formatMessage({id:"pages.account.mjSettings"}),style:{margin:"5px"},children:[(0,e.jsx)($.Z,{style:{width:"35%"},placeholder:(g=a.versionSelector)===null||g===void 0?void 0:g.placeholder,value:Y,onChange:Ae,loading:G,children:ye()}),(0,e.jsx)(Ce.Z,{wrap:!0,style:{marginTop:"5px"},children:je()})]}),(0,e.jsx)(A.Z,{type:"inner",title:i.formatMessage({id:"pages.account.nijiSettings"}),style:{margin:"5px"},children:(0,e.jsx)(Ce.Z,{wrap:!0,style:{marginTop:"5px"},children:Oe()})})]})},He=Ve,Je=s(40056),Ye=function(d){var g=d.form,a=d.onSubmit,l=d.record,r=(0,ae.useIntl)();return(0,u.useEffect)(function(){g.setFieldsValue(l)}),(0,e.jsxs)(n.Z,{form:g,labelAlign:"left",layout:"horizontal",labelCol:{span:8},wrapperCol:{span:16},onFinish:a,children:[(0,e.jsxs)(be.Z,{gutter:16,children:[(0,e.jsx)(ie.Z,{span:12,children:(0,e.jsxs)(A.Z,{type:"inner",title:r.formatMessage({id:"pages.account.info"}),children:[(0,e.jsx)(n.Z.Item,{label:"id",name:"id",hidden:!0,children:(0,e.jsx)(p.Z,{})}),(0,e.jsx)(n.Z.Item,{label:r.formatMessage({id:"pages.account.guildId"}),name:"guildId",rules:[{required:!0}],children:(0,e.jsx)(p.Z,{disabled:!0})}),(0,e.jsx)(n.Z.Item,{label:r.formatMessage({id:"pages.account.channelId"}),name:"channelId",rules:[{required:!0}],children:(0,e.jsx)(p.Z,{disabled:!0})}),(0,e.jsx)(n.Z.Item,{label:r.formatMessage({id:"pages.account.userToken"}),name:"userToken",rules:[{required:!0}],children:(0,e.jsx)(p.Z,{})}),(0,e.jsx)(n.Z.Item,{label:r.formatMessage({id:"pages.account.botToken"}),name:"botToken",rules:[{required:!0}],children:(0,e.jsx)(p.Z,{})}),(0,e.jsx)(n.Z.Item,{label:r.formatMessage({id:"pages.account.mjChannelId"}),name:"privateChannelId",children:(0,e.jsx)(p.Z,{})}),(0,e.jsx)(n.Z.Item,{label:r.formatMessage({id:"pages.account.nijiChannelId"}),name:"nijiBotChannelId",children:(0,e.jsx)(p.Z,{})}),(0,e.jsx)(n.Z.Item,{label:"User Agent",name:"userAgent",children:(0,e.jsx)(p.Z,{})}),(0,e.jsx)(n.Z.Item,{label:r.formatMessage({id:"pages.enable"}),name:"enable",valuePropName:"checked",children:(0,e.jsx)(ce.Z,{})}),(0,e.jsx)(n.Z.Item,{label:r.formatMessage({id:"pages.account.remixAutoSubmit"}),name:"remixAutoSubmit",valuePropName:"checked",children:(0,e.jsx)(ce.Z,{})})]})}),(0,e.jsx)(ie.Z,{span:12,children:(0,e.jsxs)(A.Z,{type:"inner",title:r.formatMessage({id:"pages.account.otherInfo"}),children:[(0,e.jsx)(n.Z.Item,{label:r.formatMessage({id:"pages.account.coreSize"}),name:"coreSize",children:(0,e.jsx)(k.Z,{min:1})}),(0,e.jsx)(n.Z.Item,{label:r.formatMessage({id:"pages.account.queueSize"}),name:"queueSize",children:(0,e.jsx)(k.Z,{min:1})}),(0,e.jsx)(n.Z.Item,{label:r.formatMessage({id:"pages.account.maxQueueSize"}),name:"maxQueueSize",children:(0,e.jsx)(k.Z,{min:1})}),(0,e.jsx)(n.Z.Item,{label:r.formatMessage({id:"pages.account.mode"}),name:"mode",children:(0,e.jsxs)($.Z,{allowClear:!0,children:[(0,e.jsx)($.Z.Option,{value:"RELAX",children:"RELAX"}),(0,e.jsx)($.Z.Option,{value:"FAST",children:"FAST"}),(0,e.jsx)($.Z.Option,{value:"TURBO",children:"TURBO"})]})}),(0,e.jsx)(n.Z.Item,{label:r.formatMessage({id:"pages.account.timeoutMinutes"}),name:"timeoutMinutes",children:(0,e.jsx)(k.Z,{min:1,suffix:r.formatMessage({id:"pages.minutes"})})}),(0,e.jsx)(n.Z.Item,{label:r.formatMessage({id:"pages.account.weight"}),name:"weight",children:(0,e.jsx)(k.Z,{min:1})}),(0,e.jsx)(n.Z.Item,{label:r.formatMessage({id:"pages.account.remark"}),name:"remark",children:(0,e.jsx)(p.Z,{})}),(0,e.jsx)(n.Z.Item,{label:r.formatMessage({id:"pages.account.sponsor"}),name:"sponsor",children:(0,e.jsx)(p.Z,{})})]})})]}),(0,e.jsx)(Je.Z,{message:r.formatMessage({id:"pages.account.updateNotice"}),type:"warning",style:{marginTop:"10px"}})]})},We=Ye,Qe=function(d){var g=d.form,a=d.onSubmit,l=d.record,r=(0,ae.useIntl)();return(0,u.useEffect)(function(){g.setFieldsValue(l)}),(0,e.jsx)(n.Z,{form:g,labelAlign:"left",layout:"horizontal",labelCol:{span:8},wrapperCol:{span:16},onFinish:a,children:(0,e.jsxs)(be.Z,{gutter:16,children:[(0,e.jsx)(ie.Z,{span:12,children:(0,e.jsxs)(A.Z,{type:"inner",title:r.formatMessage({id:"pages.account.info"}),children:[(0,e.jsx)(n.Z.Item,{label:"id",name:"id",hidden:!0,children:(0,e.jsx)(p.Z,{})}),(0,e.jsx)(n.Z.Item,{label:r.formatMessage({id:"pages.account.mjChannelId"}),name:"privateChannelId",children:(0,e.jsx)(p.Z,{})}),(0,e.jsx)(n.Z.Item,{label:r.formatMessage({id:"pages.account.nijiChannelId"}),name:"nijiBotChannelId",children:(0,e.jsx)(p.Z,{})}),(0,e.jsx)(n.Z.Item,{label:r.formatMessage({id:"pages.account.remixAutoSubmit"}),name:"remixAutoSubmit",valuePropName:"checked",children:(0,e.jsx)(ce.Z,{})})]})}),(0,e.jsx)(ie.Z,{span:12,children:(0,e.jsxs)(A.Z,{type:"inner",title:r.formatMessage({id:"pages.account.otherInfo"}),children:[(0,e.jsx)(n.Z.Item,{label:r.formatMessage({id:"pages.account.timeoutMinutes"}),name:"timeoutMinutes",children:(0,e.jsx)(k.Z,{min:1,suffix:r.formatMessage({id:"pages.minutes"})})}),(0,e.jsx)(n.Z.Item,{label:r.formatMessage({id:"pages.account.weight"}),name:"weight",children:(0,e.jsx)(k.Z,{min:1})}),(0,e.jsx)(n.Z.Item,{label:r.formatMessage({id:"pages.account.remark"}),name:"remark",children:(0,e.jsx)(p.Z,{})}),(0,e.jsx)(n.Z.Item,{label:r.formatMessage({id:"pages.account.sponsor"}),name:"sponsor",children:(0,e.jsx)(p.Z,{})})]})})]})})},Xe=Qe,Ge=s(94149),Ke=s(99011),qe=function(d,g){return u.createElement(le.Z,(0,F.Z)((0,F.Z)({},d),{},{ref:g,icon:Ke.Z}))},_e=u.forwardRef(qe),ea=_e,aa={icon:{tag:"svg",attrs:{viewBox:"64 64 896 896",focusable:"false"},children:[{tag:"path",attrs:{d:"M876.6 239.5c-.5-.9-1.2-1.8-2-2.5-5-5-13.1-5-18.1 0L684.2 409.3l-67.9-67.9L788.7 169c.8-.8 1.4-1.6 2-2.5 3.6-6.1 1.6-13.9-4.5-17.5-98.2-58-226.8-44.7-311.3 39.7-67 67-89.2 162-66.5 247.4l-293 293c-3 3-2.8 7.9.3 11l169.7 169.7c3.1 3.1 8.1 3.3 11 .3l292.9-292.9c85.5 22.8 180.5.7 247.6-66.4 84.4-84.5 97.7-213.1 39.7-311.3zM786 499.8c-58.1 58.1-145.3 69.3-214.6 33.6l-8.8 8.8-.1-.1-274 274.1-79.2-79.2 230.1-230.1s0 .1.1.1l52.8-52.8c-35.7-69.3-24.5-156.5 33.6-214.6a184.2 184.2 0 01144-53.5L537 318.9a32.05 32.05 0 000 45.3l124.5 124.5a32.05 32.05 0 0045.3 0l132.8-132.8c3.7 51.8-14.4 104.8-53.6 143.9z"}}]},name:"tool",theme:"outlined"},na=aa,sa=function(d,g){return u.createElement(le.Z,(0,F.Z)((0,F.Z)({},d),{},{ref:g,icon:na}))},ta=u.forwardRef(sa),ra=ta,ia=s(27363),oa=function(d,g){return u.createElement(le.Z,(0,F.Z)((0,F.Z)({},d),{},{ref:g,icon:ia.Z}))},la=u.forwardRef(oa),ca=la,ua={icon:{tag:"svg",attrs:{viewBox:"64 64 896 896",focusable:"false"},children:[{tag:"path",attrs:{d:"M678.3 642.4c24.2-13 51.9-20.4 81.4-20.4h.1c3 0 4.4-3.6 2.2-5.6a371.67 371.67 0 00-103.7-65.8c-.4-.2-.8-.3-1.2-.5C719.2 505 759.6 431.7 759.6 349c0-137-110.8-248-247.5-248S264.7 212 264.7 349c0 82.7 40.4 156 102.6 201.1-.4.2-.8.3-1.2.5-44.7 18.9-84.8 46-119.3 80.6a373.42 373.42 0 00-80.4 119.5A373.6 373.6 0 00137 888.8a8 8 0 008 8.2h59.9c4.3 0 7.9-3.5 8-7.8 2-77.2 32.9-149.5 87.6-204.3C357 628.2 432.2 597 512.2 597c56.7 0 111.1 15.7 158 45.1a8.1 8.1 0 008.1.3zM512.2 521c-45.8 0-88.9-17.9-121.4-50.4A171.2 171.2 0 01340.5 349c0-45.9 17.9-89.1 50.3-121.6S466.3 177 512.2 177s88.9 17.9 121.4 50.4A171.2 171.2 0 01683.9 349c0 45.9-17.9 89.1-50.3 121.6C601.1 503.1 558 521 512.2 521zM880 759h-84v-84c0-4.4-3.6-8-8-8h-56c-4.4 0-8 3.6-8 8v84h-84c-4.4 0-8 3.6-8 8v56c0 4.4 3.6 8 8 8h84v84c0 4.4 3.6 8 8 8h56c4.4 0 8-3.6 8-8v-84h84c4.4 0 8-3.6 8-8v-56c0-4.4-3.6-8-8-8z"}}]},name:"user-add",theme:"outlined"},da=ua,ma=function(d,g){return u.createElement(le.Z,(0,F.Z)((0,F.Z)({},d),{},{ref:g,icon:da}))},ga=u.forwardRef(ma),fa=ga,ha=s(90930),pa=s(72051),va=s(28248),ja=function(){var d=(0,u.useState)(!1),g=h()(d,2),a=g[0],l=g[1],r=(0,u.useState)(!1),V=h()(r,2),C=V[0],_=V[1],H=(0,u.useState)((0,e.jsx)(e.Fragment,{})),J=h()(H,2),Y=J[0],D=J[1],W=(0,u.useState)(""),B=h()(W,2),ne=B[0],N=B[1],se=(0,u.useState)(1e3),Q=h()(se,2),te=Q[0],L=Q[1],X=n.Z.useForm(),y=h()(X,1),G=y[0],c=re.ZP.useNotification(),he=h()(c,2),K=he[0],de=he[1],pe=(0,u.useState)(!1),i=h()(pe,2),ve=i[0],R=i[1],f=(0,ae.useIntl)(),ye=(0,u.useState)([]),je=h()(ye,2),Oe=je[0],Ae=je[1],xe=(0,u.useState)(!1),T=h()(xe,2),o=T[0],M=T[1],w=function(){var Z=z()(x()().mark(function v(){var t;return x()().wrap(function(b){for(;;)switch(b.prev=b.next){case 0:return M(!0),b.next=3,(0,P.Ed)();case 3:t=b.sent,Ae(t),M(!1);case 6:case"end":return b.stop()}},v)}));return function(){return Z.apply(this,arguments)}}(),E=function(v,t,j){G.resetFields(),N(v),D(t),L(j),l(!0)},S=function(){D((0,e.jsx)(e.Fragment,{})),l(!1),_(!1)},q=(0,e.jsxs)(e.Fragment,{children:[(0,e.jsx)(O.ZP,{onClick:S,children:f.formatMessage({id:"pages.cancel"})},"back"),(0,e.jsx)(O.ZP,{type:"primary",loading:ve,onClick:function(){return G.submit()},children:f.formatMessage({id:"pages.submit"})},"submit")]}),ee=function(){S(),w()},Za=function(){var Z=z()(x()().mark(function v(t){var j;return x()().wrap(function(I){for(;;)switch(I.prev=I.next){case 0:return R(!0),I.next=3,(0,P.o1)(t);case 3:j=I.sent,j.success?(K.success({message:"success",description:j.message}),S(),ee()):(K.error({message:"error",description:j.message}),ee()),R(!1);case 6:case"end":return I.stop()}},v)}));return function(t){return Z.apply(this,arguments)}}(),Ma=function(){var Z=z()(x()().mark(function v(t){var j;return x()().wrap(function(I){for(;;)switch(I.prev=I.next){case 0:return R(!0),I.next=3,(0,P.MS)(t.id,t);case 3:j=I.sent,j.success?K.success({message:"success",description:j.message}):K.error({message:"error",description:j.message}),S(),ee(),R(!1);case 8:case"end":return I.stop()}},v)}));return function(t){return Z.apply(this,arguments)}}(),Ia=function(){var Z=z()(x()().mark(function v(t){var j;return x()().wrap(function(I){for(;;)switch(I.prev=I.next){case 0:return R(!0),I.next=3,(0,P.Vx)(t.id,t);case 3:j=I.sent,j.success?K.success({message:"success",description:j.message}):K.error({message:"error",description:j.message}),S(),ee(),R(!1);case 8:case"end":return I.stop()}},v)}));return function(t){return Z.apply(this,arguments)}}(),Sa=[{title:f.formatMessage({id:"pages.account.guildId"}),dataIndex:"guildId",width:200,align:"center",render:function(v,t){return(0,e.jsx)("a",{onClick:function(){_(!0),E(f.formatMessage({id:"pages.account.info"})+" - "+t.id,(0,e.jsx)(He,{record:t,onSuccess:ee}),1100)},children:v})}},{title:f.formatMessage({id:"pages.account.channelId"}),dataIndex:"channelId",align:"center",width:200},{title:"".concat(f.formatMessage({id:"pages.account.status"})),dataIndex:"enable",width:200,align:"center",request:function(){var Z=z()(x()().mark(function t(){return x()().wrap(function(b){for(;;)switch(b.prev=b.next){case 0:return b.abrupt("return",[{label:f.formatMessage({id:"pages.enable"}),value:"true"},{label:f.formatMessage({id:"pages.disable"}),value:"false"}]);case 1:case"end":return b.stop()}},t)}));function v(){return Z.apply(this,arguments)}return v}(),render:function(v,t){var j=v?"green":"volcano",b=v?f.formatMessage({id:"pages.enable"}):f.formatMessage({id:"pages.disable"});return(0,e.jsxs)(e.Fragment,{children:[(0,e.jsx)(oe.Z,{color:j,children:b}),t.lock&&(0,e.jsx)(oe.Z,{icon:(0,e.jsx)(Ge.Z,{}),color:"warning",children:(0,e.jsx)(ue.Z,{title:f.formatMessage({id:"pages.account.lockmsg"}),children:f.formatMessage({id:"pages.account.lock"})})}),v&&!t.running&&(0,e.jsx)(oe.Z,{icon:(0,e.jsx)(fe,{}),color:"error",children:f.formatMessage({id:"pages.account.notRunning"})}),(t==null?void 0:t.runningCount)>0&&(0,e.jsx)(oe.Z,{icon:(0,e.jsx)(fe,{spin:!0}),color:"cyan",children:(0,e.jsx)(ue.Z,{title:f.formatMessage({id:"pages.account.runningCount"})+" "+t.runningCount,children:(t==null?void 0:t.runningCount)||0})}),(t==null?void 0:t.queueCount)>0&&(0,e.jsx)(oe.Z,{icon:(0,e.jsx)(ea,{}),color:"processing",children:(0,e.jsx)(ue.Z,{title:f.formatMessage({id:"pages.account.queueCount"})+" "+t.queueCount,children:t.queueCount||0})})]})}},{title:f.formatMessage({id:"pages.account.fastTimeRemaining"}),dataIndex:"fastTimeRemaining",ellipsis:!0,width:200,hideInSearch:!0,align:"center"},{title:f.formatMessage({id:"pages.account.renewDate"}),dataIndex:"renewDate",align:"center",width:160,hideInSearch:!0,render:function(v,t){return t.displays.renewDate}},{title:f.formatMessage({id:"pages.account.sponsor"}),dataIndex:"sponsor",ellipsis:!0,width:100,render:function(v,t){return(0,e.jsx)("div",{dangerouslySetInnerHTML:{__html:t.sponsor||"-"}})}},{title:f.formatMessage({id:"pages.account.remark"}),dataIndex:"remark",ellipsis:!0,width:150},{title:f.formatMessage({id:"pages.account.disabledReason"}),dataIndex:"disabledReason",ellipsis:!0,width:150,hideInSearch:!0},{title:f.formatMessage({id:"pages.operation"}),dataIndex:"operation",width:200,key:"operation",fixed:"right",hideInSearch:!0,render:function(v,t){return(0,e.jsxs)(Ce.Z,{children:[(0,e.jsx)(Le,{record:t,onSuccess:ee}),(0,e.jsx)(ue.Z,{title:f.formatMessage({id:"pages.account.updateAndReconnect"}),children:(0,e.jsx)(O.ZP,{type:"primary",icon:(0,e.jsx)(ra,{}),onClick:function(){return E(f.formatMessage({id:"pages.account.updateAndReconnect"}),(0,e.jsx)(We,{form:G,record:t,onSubmit:Ma}),1e3)}},"EditAndReconnect")}),(0,e.jsx)(O.ZP,{icon:(0,e.jsx)(ca,{}),onClick:function(){return E(f.formatMessage({id:"pages.account.update"}),(0,e.jsx)(Xe,{form:G,record:t,onSubmit:Ia}),1e3)}},"Update"),(0,e.jsx)(Te,{record:t,onSuccess:ee})]})}}];return(0,u.useEffect)(function(){w()},[]),(0,e.jsxs)(ha._z,{children:[de,(0,e.jsxs)(A.Z,{children:[(0,e.jsxs)("div",{style:{display:"flex",justifyContent:"flex-end",marginBottom:16},children:[(0,e.jsx)(O.ZP,{type:"primary",icon:(0,e.jsx)(fa,{}),onClick:function(){E(f.formatMessage({id:"pages.account.add"}),(0,e.jsx)(Pe,{form:G,onSubmit:Za}),1e3)},children:f.formatMessage({id:"pages.add"})},"primary"),(0,e.jsx)(O.ZP,{type:"default",style:{marginLeft:8},icon:(0,e.jsx)(fe,{}),onClick:function(){ee()}})]}),(0,e.jsx)(pa.Z,{scroll:{x:1e3},rowKey:"id",columns:Sa,dataSource:Oe,loading:o,pagination:!1})]}),(0,e.jsx)(va.Z,{title:ne,open:a,onCancel:S,footer:C?null:q,width:te,children:Y})]})},xa=ja}}]);
