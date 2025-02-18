"use strict";(self.webpackChunkmidjourney_proxy_admin=self.webpackChunkmidjourney_proxy_admin||[]).push([[971],{23391:function(re,U,s){s.r(U),s.d(U,{default:function(){return te}});var H=s(15009),E=s.n(H),$=s(99289),W=s.n($),k=s(5574),h=s.n(k),u=s(67294),K=s(74981),oe=s(90252),ge=s(22777),e=s(85893),X=function(y){var C=y.value,g=C===void 0?{}:C,a=y.onChange,P=u.useState(JSON.stringify(g,null,2)),v=h()(P,2),x=v[0],d=v[1],I=u.useState(!0),T=h()(I,2),F=T[0],M=T[1];(0,u.useEffect)(function(){var L=JSON.stringify(g),Z=JSON.stringify(JSON.parse(x));L!==Z&&d(JSON.stringify(g,null,2))},[g]);var b=function(Z){d(Z);try{var A=JSON.parse(Z);M(!0),a&&a(A)}catch(B){M(!1)}};return(0,e.jsxs)("div",{style:{width:"100%"},children:[(0,e.jsx)(K.ZP,{mode:"json",theme:"textmate",value:x,onChange:b,name:"json-editor",editorProps:{$blockScrolling:!0},height:"auto",maxLines:1/0,setOptions:{enableBasicAutocompletion:!0,enableLiveAutocompletion:!0,enableSnippets:!0,showLineNumbers:!0,tabSize:2,useWorker:!1},style:{width:"100%",minHeight:"80px"},fontSize:14,lineHeight:19,showPrintMargin:!0,showGutter:!0,highlightActiveLine:!0}),!F&&(0,e.jsx)("div",{style:{color:"red",marginTop:"8px"},children:"JSON \u683C\u5F0F\u9519\u8BEF\uFF0C\u8BF7\u68C0\u67E5\u8F93\u5165\uFF01"})]})},l=X,S=s(66927),Y=s(60219),w=s(90930),q=s(94272),t=s(53025),c=s(2453),_=s(74330),V=s(42075),J=s(40056),ee=s(83062),m=s(26915),O=s(28036),z=s(71230),D=s(15746),N=s(4393),i=s(72269),f=s(37804),n=s(74656),ae=function(){var y=t.Z.useForm(),C=h()(y,1),g=C[0],a=(0,q.useIntl)(),P=(0,u.useState)(!1),v=h()(P,2),x=v[0],d=v[1],I=function(){d(!0),(0,S.iE)().then(function(r){d(!1),r.success&&g.setFieldsValue(r.data)})};(0,u.useEffect)(function(){I()},[]);var T=function(){g.validateFields().then(function(r){d(!0),(0,S.rF)(r).then(function(p){d(!1),p.success?(c.ZP.success(a.formatMessage({id:"pages.setting.saveSuccess"})),I()):c.ZP.error(p.message||a.formatMessage({id:"pages.setting.error"}))})}).catch(function(){c.ZP.error(a.formatMessage({id:"pages.setting.error"}))})},F=(0,u.useState)(""),M=h()(F,2),b=M[0],L=M[1],Z=(0,u.useState)(""),A=h()(Z,2),B=A[0],ne=A[1],ie=function(){var j=W()(E()().mark(function r(p,G){var Q,R;return E()().wrap(function(o){for(;;)switch(o.prev=o.next){case 0:return o.prev=0,d(!0),Q={Host:p,ApiSecret:G},o.next=5,(0,S.gU)(Q);case 5:R=o.sent,R.success?c.ZP.success(a.formatMessage({id:"pages.setting.migrateSuccess"})):c.ZP.error(R.message),o.next=12;break;case 9:o.prev=9,o.t0=o.catch(0),c.ZP.error(o.t0);case 12:return o.prev=12,d(!1),o.finish(12);case 15:case"end":return o.stop()}},r,null,[[0,9,12,15]])}));return function(p,G){return j.apply(this,arguments)}}(),le=function(){b?ie(b,B):c.ZP.warning(a.formatMessage({id:"pages.setting.migrateTips"}))};return(0,e.jsx)(w._z,{children:(0,e.jsx)(t.Z,{form:g,labelAlign:"left",layout:"horizontal",labelCol:{span:6},wrapperCol:{span:18},children:(0,e.jsxs)(_.Z,{spinning:x,children:[(0,e.jsxs)(V.Z,{style:{marginBottom:"10px",display:"flex",justifyContent:"space-between"},children:[(0,e.jsx)(J.Z,{type:"info",style:{paddingTop:"4px",paddingBottom:"4px"},description:a.formatMessage({id:"pages.setting.tips"})}),(0,e.jsxs)(V.Z,{children:[(0,e.jsx)(ee.Z,{placement:"bottom",title:(0,e.jsxs)("div",{style:{display:"flex",flexDirection:"column",gap:8,padding:8},children:[(0,e.jsx)(m.Z,{style:{marginBottom:8},placeholder:"mjplus host",value:b,onChange:function(r){return L(r.target.value)}}),(0,e.jsx)(m.Z,{placeholder:"mj-api-secret",value:B,onChange:function(r){return ne(r.target.value)}})]}),children:(0,e.jsx)(O.ZP,{loading:x,type:"primary",ghost:!0,onClick:le,children:a.formatMessage({id:"pages.setting.migrate"})})}),(0,e.jsx)(O.ZP,{loading:x,icon:(0,e.jsx)(Y.Z,{}),type:"primary",onClick:T,children:a.formatMessage({id:"pages.setting.save"})})]})]}),(0,e.jsxs)(z.Z,{gutter:16,children:[(0,e.jsx)(D.Z,{span:12,children:(0,e.jsxs)(N.Z,{title:a.formatMessage({id:"pages.setting.accountSetting"}),bordered:!1,children:[(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.enableSwagger"}),name:"enableSwagger",extra:g.getFieldValue("enableSwagger")?(0,e.jsx)("a",{href:"/swagger",target:"_blank",rel:"noreferrer",children:a.formatMessage({id:"pages.setting.swaggerLink"})}):"",children:(0,e.jsx)(i.Z,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.mongoDefaultConnectionString"}),name:"mongoDefaultConnectionString",extra:(0,e.jsxs)(e.Fragment,{children:[(0,e.jsx)(O.ZP,{style:{marginTop:8},type:"primary",onClick:function(){var r=g.getFieldValue("mongoDefaultConnectionString");r?(d(!0),(0,S.yk)().then(function(p){d(!1),p.success?c.ZP.success(a.formatMessage({id:"pages.setting.connectSuccess"})):c.ZP.error(p.message||a.formatMessage({id:"pages.setting.connectError"}))})):c.ZP.warning(a.formatMessage({id:"pages.setting.mongoDefaultConnectionStringTips"}))},children:a.formatMessage({id:"pages.setting.testConnect"})}),g&&!g.getFieldValue("isMongo")&&(0,e.jsx)(J.Z,{style:{marginTop:8},message:a.formatMessage({id:"pages.setting.mongoNotUsed"}),type:"warning"})]}),children:(0,e.jsx)(m.Z,{placeholder:"mongodb://mongoadmin:***admin@192.168.x.x"})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.mongoDefaultDatabase"}),name:"mongoDefaultDatabase",children:(0,e.jsx)(m.Z,{placeholder:"mj"})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.isMongoAutoMigrate"}),name:"isMongoAutoMigrate",tooltip:a.formatMessage({id:"pages.setting.isMongoAutoMigrateTips"}),children:(0,e.jsx)(i.Z,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.maxCount"}),name:"maxCount",children:(0,e.jsx)(f.Z,{min:-1})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.databaseType"}),name:"databaseType",children:(0,e.jsxs)(n.Z,{allowClear:!0,children:[(0,e.jsx)(n.Z.Option,{value:"LiteDB",children:"LiteDB"}),(0,e.jsx)(n.Z.Option,{value:"MongoDB",children:"MongoDB"}),(0,e.jsx)(n.Z.Option,{value:"SQLite",children:"SQLite"}),(0,e.jsx)(n.Z.Option,{value:"MySQL",children:"MySQL"}),(0,e.jsx)(n.Z.Option,{value:"PostgreSQL",children:"PostgreSQL"}),(0,e.jsx)(n.Z.Option,{value:"SQLServer",children:"SQLServer"})]})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.databaseConnectionString"}),name:"databaseConnectionString",children:(0,e.jsx)(m.Z,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.databaseName"}),name:"databaseName",children:(0,e.jsx)(m.Z,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.accountChooseRule"}),name:"accountChooseRule",children:(0,e.jsxs)(n.Z,{allowClear:!0,children:[(0,e.jsx)(n.Z.Option,{value:"BestWaitIdle",children:"BestWaitIdle"}),(0,e.jsx)(n.Z.Option,{value:"Random",children:"Random"}),(0,e.jsx)(n.Z.Option,{value:"Weight",children:"Weight"}),(0,e.jsx)(n.Z.Option,{value:"Polling",children:"Polling"})]})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.discordConfig"}),name:"ngDiscord",children:(0,e.jsx)(l,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.proxyConfig"}),name:"proxy",children:(0,e.jsx)(l,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.imageStorageType"}),name:"imageStorageType",children:(0,e.jsxs)(n.Z,{allowClear:!0,children:[(0,e.jsx)(n.Z.Option,{value:"NONE",children:"NULL"}),(0,e.jsx)(n.Z.Option,{value:"LOCAL",children:"LOCAL"}),(0,e.jsx)(n.Z.Option,{value:"OSS",children:"Aliyun OSS"}),(0,e.jsx)(n.Z.Option,{value:"COS",children:"Tencent COS"}),(0,e.jsx)(n.Z.Option,{value:"R2",children:"Cloudflare R2"})]})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.localStorage"}),name:"localStorage",children:(0,e.jsx)(l,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.aliyunOss"}),name:"aliyunOss",children:(0,e.jsx)(l,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.tencentCos"}),name:"tencentCos",children:(0,e.jsx)(l,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.cloudflareR2"}),name:"cloudflareR2",children:(0,e.jsx)(l,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.replicate"}),name:"replicate",children:(0,e.jsx)(l,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.translate"}),name:"translateWay",children:(0,e.jsxs)(n.Z,{allowClear:!0,children:[(0,e.jsx)(n.Z.Option,{value:"NULL",children:"NULL"}),(0,e.jsx)(n.Z.Option,{value:"BAIDU",children:"BAIDU"}),(0,e.jsx)(n.Z.Option,{value:"GPT",children:"GPT"})]})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.baiduTranslate"}),name:"baiduTranslate",children:(0,e.jsx)(l,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.openai"}),name:"openai",children:(0,e.jsx)(l,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.smtp"}),name:"smtp",children:(0,e.jsx)(l,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.notifyHook"}),name:"notifyHook",children:(0,e.jsx)(m.Z,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.notifyPoolSize"}),name:"notifyPoolSize",children:(0,e.jsx)(f.Z,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.captchaServer"}),name:"captchaServer",help:a.formatMessage({id:"pages.setting.captchaServerTip"}),children:(0,e.jsx)(m.Z,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.captchaNotifyHook"}),name:"captchaNotifyHook",help:a.formatMessage({id:"pages.setting.captchaNotifyHookTip"}),children:(0,e.jsx)(m.Z,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.captchaNotifySecret"}),name:"captchaNotifySecret",help:a.formatMessage({id:"pages.setting.captchaNotifySecretTip"}),children:(0,e.jsx)(m.Z,{})})]})}),(0,e.jsx)(D.Z,{span:12,children:(0,e.jsxs)(N.Z,{title:a.formatMessage({id:"pages.setting.otherSetting"}),bordered:!1,children:[(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.enableAccountSponsor"}),name:"enableAccountSponsor",help:a.formatMessage({id:"pages.setting.enableAccountSponsorTips"}),children:(0,e.jsx)(i.Z,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.isVerticalDomain"}),name:"isVerticalDomain",help:a.formatMessage({id:"pages.setting.isVerticalDomainTips"}),children:(0,e.jsx)(i.Z,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.enableRegister"}),name:"enableRegister",children:(0,e.jsx)(i.Z,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.registerUserDefaultDayLimit"}),name:"registerUserDefaultDayLimit",children:(0,e.jsx)(f.Z,{min:-1})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.registerUserDefaultTotalLimit"}),name:"registerUserDefaultTotalLimit",children:(0,e.jsx)(f.Z,{min:-1})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.registerUserDefaultCoreSize"}),name:"registerUserDefaultCoreSize",children:(0,e.jsx)(f.Z,{min:-1})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.registerUserDefaultQueueSize"}),name:"registerUserDefaultQueueSize",children:(0,e.jsx)(f.Z,{min:-1})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.enableGuest"}),name:"enableGuest",children:(0,e.jsx)(i.Z,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.guestDefaultDayLimit"}),name:"guestDefaultDayLimit",children:(0,e.jsx)(f.Z,{min:-1})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.bannedLimiting"}),name:"bannedLimiting",children:(0,e.jsx)(l,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.ipRateLimiting"}),name:"ipRateLimiting",children:(0,e.jsx)(l,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.ipBlackRateLimiting"}),name:"ipBlackRateLimiting",children:(0,e.jsx)(l,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.notify"}),name:"notify",children:(0,e.jsx)(m.Z.TextArea,{autoSize:{minRows:1,maxRows:10}})})]})})]}),(0,e.jsx)(z.Z,{gutter:16,style:{marginTop:"16px"},children:(0,e.jsx)(D.Z,{span:12,children:(0,e.jsxs)(N.Z,{title:a.formatMessage({id:"pages.setting.discordSetting"}),bordered:!1,children:[(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.enableAutoGetPrivateId"}),name:"enableAutoGetPrivateId",help:a.formatMessage({id:"pages.setting.enableAutoGetPrivateIdTips"}),children:(0,e.jsx)(i.Z,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.enableAutoVerifyAccount"}),name:"enableAutoVerifyAccount",help:a.formatMessage({id:"pages.setting.enableAutoVerifyAccountTips"}),children:(0,e.jsx)(i.Z,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.enableAutoSyncInfoSetting"}),name:"enableAutoSyncInfoSetting",help:a.formatMessage({id:"pages.setting.enableAutoSyncInfoSettingTips"}),children:(0,e.jsx)(i.Z,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.enableAutoExtendToken"}),name:"enableAutoExtendToken",help:a.formatMessage({id:"pages.setting.enableAutoExtendTokenTips"}),children:(0,e.jsx)(i.Z,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.enableUserCustomUploadBase64"}),name:"enableUserCustomUploadBase64",help:a.formatMessage({id:"pages.setting.enableUserCustomUploadBase64Tips"}),children:(0,e.jsx)(i.Z,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.enableConvertOfficialLink"}),name:"enableConvertOfficialLink",help:a.formatMessage({id:"pages.setting.enableConvertOfficialLinkTips"}),children:(0,e.jsx)(i.Z,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.enableConvertAliyunLink"}),name:"enableConvertAliyunLink",help:a.formatMessage({id:"pages.setting.enableConvertAliyunLinkTips"}),children:(0,e.jsx)(i.Z,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.enableMjTranslate"}),name:"enableMjTranslate",help:a.formatMessage({id:"pages.setting.enableMjTranslateTips"}),children:(0,e.jsx)(i.Z,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.enableNijiTranslate"}),name:"enableNijiTranslate",help:a.formatMessage({id:"pages.setting.enableNijiTranslateTips"}),children:(0,e.jsx)(i.Z,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.enableConvertNijiToMj"}),name:"enableConvertNijiToMj",help:a.formatMessage({id:"pages.setting.enableConvertNijiToMjTips"}),children:(0,e.jsx)(i.Z,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.enableConvertNijiToNijiBot"}),name:"enableConvertNijiToNijiBot",help:a.formatMessage({id:"pages.setting.enableConvertNijiToNijiBotTips"}),children:(0,e.jsx)(i.Z,{})}),(0,e.jsx)(t.Z.Item,{label:a.formatMessage({id:"pages.setting.enableAutoLogin"}),name:"enableAutoLogin",help:a.formatMessage({id:"pages.setting.enableAutoLoginTips"}),children:(0,e.jsx)(i.Z,{})})]})})})]})})})},te=ae}}]);
