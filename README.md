# Catalyst

此框架是面向爬虫任务的异步任务调度管理框架。目标是让业务逻辑（爬虫逻辑）开发能够完全聚焦业务本身，
而不需要考虑任何网络错误、重试、延时等非业务细节，从而全面提高业务扩展效率，同时框架作为底层能统一保障系统稳定性。

框架技术特点为：
- 完全接口化控制任务配置/调度等所有系统行为
- 二级并发控制
- 完善的时间控制机制：任务延迟、重试次数、指数放大重试间隔、随机扰动
- 优先级和超时控制
- 灵活的插件机制


## 1. 总览

程序主入口在`src/server.js`，运行`node src/server.js`或者`npm start`即可启动。
程序启动前，可以按需先通过`src/config.js`修改系统环境配置。

框架所支撑的业务逻辑默认统一放在`src/tasks`路径下。
业务逻辑的代码结构一般是层级结构，第一层为文件夹，文件夹的名字定义**业务域**；
该业务域下可以任意组织目录结构，系统会搜索所有不以`_`开头的`.js`文件，当做**任务类型定义**加载到系统中。以下是一个简单例子：
```
src/
├─ tasks/
│  ├─ xiaohongshu/
   │  ├─ _utils.js
   │  ├─ all.js
   │  ├─ kol.js
   │  └─ item/
   │     ├─ influencer.js
   │     └─ public.js
   └─ toutiao/
      ├─ kol.js
      └─ item_detail.js
```
系统加载时，会识别出两个业务域`xiaohongshu`和`toutiao`，以及6个任务类型定义：
- `xiaohongshu.all`
- `xiaohongshu.kol`
- `xiaohongshu.item.influencer`
- `xiaohongshu.item.public`
- `toutiao.kol`
- `toutiao.item`

下文中所有提及的**配置项**均可在`src/service/schema.graphql`中找到。

## 2. 概念

#### 2.1 业务域（domain，`TaskDomainConfig`）

业务逻辑目录下第一级目录的名字，定义业务上的一个范围。它除了是一个名字之外，本身还控制着域级别的配置项，如`TaskDomainConfig.maxConcurrency`。
所有该业务域下的任务定义，若任务本身没有专门设置配置项的值，默认会参考域级别的配置。

定义一个业务域除了是目录名字外，还可以在目录下添加一个名为`domain.js`的模块，其格式为
```javascript
module.exports = {
    config: { // 任务域级别的配置项
        maxConcurrency: 1
    },
    plugins: { // 要启用的插件和其配置
        request: {
            defaults: {headers: {'accept': '*/*'}},
            singleton: true
        }
    },
    async store() { // 全局连续的变量空间的初始化，只在整个系统初始化时调用
        return {storedValue : 1};
    }
};
```
模块中每一个字段都是可选的。

- `config`  
  业务域配置，具体参照`TaskTypeConfig`。该字段也可以是一个能够返回所述对象的sync/async function。
  对于没有设置的配置项，会默认使用系统代码级别的默认配置，
  参照`src/config.js`中的`DEFAULT_TASK_DOMAIN_CONFIG`。

- `plugins`  
  要启用的插件和插件的配置，形式如`{plugin1: {...configs}, plugin2: {...}}`。
  该字段也可以是一个能够返回所述对象的sync/async function。
  插件能够允许业务逻辑中以类似`this.plugin1.someMethod()`的方式来使用其能力。详见[插件](#3-)。

- `store`  
  一个全局连续的变量空间，此字段要求是一个**原生js对象**，或者是一个能够返回这样对象的sync/async function。
  该js对象表现类似于一个变量空间，其中可以放置任意的属性项和值，用于业务逻辑执行时使用。  
  注意这个`store`是在**整个业务域全局共享**的，它在整个系统的生命周期中只会初始化一次，
  后续任何该域下的任务执行时对store里属性的修改都会立刻反映到所有其它任务中。  
  `store`建议的使用方式为初始环境变量的设置，例如一个业务域下所有任务都是web网站爬虫，那么`store`中可以初始化一个`browser`的实例，
  从而避免每次任务执行时重复创建，节约开销的同时可以共享一些cookies信息，如登录状态等。  
  `store`不可以是非原生js对象（比如直接是一个`browser`实例），不然会造成系统崩溃。


#### 2.2 任务类型定义（taskType，`TaskTypeConfig`）

任务类型定义指代的是一段业务逻辑。它的表现形式是一个`.js`的模块，其形式为
```javascript
module.exports = {
    config: { // 任务类型级别的配置项，用于覆盖业务域的默认配置
        concurrency: 1
    },
    plugins: { // 要启用的插件和其配置，会继承业务域的配置，此处可以补充插件或覆盖配置
        request: {
            defaults: {headers: {'accept': '*/*'}},
            singleton: true
        }
    },
    async store() { // 全局连续的变量空间的初始化，只在整个系统初始化时调用
        return {storedValue : 1};
    },
    async run(params, context, store) { // 核心的业务逻辑在此实现
        this.info('hello world!');
    },
    async failed(code, message, params, context, store) { // 错误处理
        this.error('an error has occurred');
    },
    async catch(err, params, context, store) {
        return err.name === 'TimeoutError';
    },
    async final(params, context, store) { // 执行完成时的收尾
        this.info('task final is called no matter the task is success / failed / canceled.');
    }
};
```
模块中每一个字段都是可选的。

- `config`  
  用于覆盖业务域（`TaskDomainConfig`）的默认配置，具体参照`TaskTypeConfig`。
  该字段也可以是一个能够返回所述对象的sync/async function。
  没有设置的配置项，会默认使用业务域的配置。

- `plugins`  
  与业务域中的`plugins`类似。任务执行时，可以使用业务域级别定义的插件，同时可以使用这里补充定义的插件。
  该字段也可以是一个能够返回所述对象的sync/async function。
  当然这里也可以用于覆盖业务域插件的配置。插件的具体用法，详见[插件](#3-)。

- `store`  
  与业务域中的`store`类似。如果任务类型中定义了`store`，真正任务执行时所看到的`store`会是业务域和此定义内容的**融合**，
  类似于`store = {...domain.store, ...taskTypeConfig.store}`。
  
- `run`  
  主业务逻辑，在这里可以做任何想做的事情。`run`可以是一个sync/async function，它接收3个参数：`params`，`context`，`store`。  
  `params`是一个**只读**的**原生js对象**，里面的信息为任务执行的业务参数，具体有哪些参数完全由业务逻辑的开发者（撰写这个js文件的人）制定。  
  例如，一个爬取小红书全站的任务，可以定义`params = {pageStart: 1, pageEnd: 20}`，表示任务只爬取网站第1~20页的数据。  
  `context`是一个**可读写**的**原生js对象**，里面存储的是任务执行的状态。最直接的一种用法，就是把本来在`run`函数中所有可能声明的变量，
  与其用`let a = ...`，全部改写成`context.a = ...`。`run`的业务逻辑被调用除了是为了执行一个新任务外，还有可能是以前一个任务的**恢复**，
  例如上文提到的爬取小红书全站的任务，它的参数`params`规定了这次任务爬取的页面范围，
  那么它的实现逻辑可以在`context`中创建一个变量，例如`context.curPage = 1`，用于表示当前任务正在爬取的页码。如果因为某些系统原因造成宕机，
  `context`的内容会被保留在系统存储中，这样当系统恢复执行时会重新调起此任务并设置上次运行的`context`状态。
  因此，**建议所有任务的`run`实现逻辑都是基于断点续传的思想**，先判断`context`中是否有状态，再按需执行任务，这样是最高效使用整个框架的方法。  
  `store`为全局变量，详见上文。  
  在该函数中，可以通过`this`调用系统自带的一些方法，可参照`src/task.js`中`class Job`下所有不以`_`开头的方法。
  常用的方法有`this.schedule`调起其他任务、日志打印`this.info`/`this.warn`/`this.error`、
  错误上报`this.fail`、取消执行`this.cancel`等，详见[任务方法](#3-)。

- `failed`  
  错误处理逻辑，这个方法当且仅当在`run`中通过`this.fail`方法**主动**报出错误时才会被调用，意思是业务逻辑中出现了**预期**但不可处理的错误，
  例如爬虫时目标网站访问不了等，此方法可以针对这样的错误采取一些处理，之后系统会根据重试的配置自动对此任务进行**重试**。  
  如果是系统错误，如业务逻辑代码不鲁棒执行了类似`let a = undefined; a.value = 5;`的语句从而抛出了异常，
  系统会捕获这个错误、记录异常，但**不会**执行`failed`，也**不会**触发重试等机制。因为这样的错误会被认为是内部问题，无法通过现有业务逻辑处理，而应该改进代码，
  使其正确执行或者通过`this.fail`标记为业务逻辑异常。  
  
- `catch`
  有时候随着业务逻辑的复杂，用`this.fail`一个一个上报错误也会比较麻烦，为了能够让符合一些特征的错误被定性为业务预期的错误，
  可以用此方法对产生的错误对象进行判断，返回一个`true`/`false`的值来表示这是否一个预期错误。
  若返回`true`，那么此错误就像是被执行了`this.fail`一样。

- `final`
  收尾逻辑，这个方法在任务执行最后**一定**会被调用，不管是出现了业务错误还是系统错误。目标是收尾和回收资源，例如关闭网站的链接，等。


#### 2.3 任务配置（task，`Task`）

具体的一个任务的定义。它与任务定义不同之处在于，它是由**任务类型**、**任务参数**和**任务调度参数**组成，用来表示一次性或者周期性执行某种任务的配置。
例如，定义一个任务，类型是`xiaohongshu.all`（爬取小红书全站），参数是`{pageStart: 1, pageEnd: 20}`，调度参数是`{schedule: "* * 1 * *"}`,
那么系统就会每个月1号执行一次爬取小红书第1~20页的数据。


#### 2.4 任务实例（job，`Job`）

具体的一次任务的执行，相当于是最细粒度的系统的行为记录。以上文爬取小红书全站为例，每个月1号系统执行该任务时，就会生成一个任务实例，
记录这一次任务执行开始、结束的时间，任务的状态等。


换句话说，任务类型定义`TaskTypeConfig`类比一个function，任务配置`Task`则是一个`{fn, params, schedule}`的记录，
任务实例`Job`则是一次function call，类似`fn(params)`。


## 3. 任务自带的方法

以下列出所有系统自带的方法。请注意有些是异步方法，需要加`await`，错误使用会导致不可预期的结果。

- `await this.schedule(taskTypeFullName, params)`  
  调度执行一次子任务。  
  任务类型定义`taskTypeFullName`按照文首所说的格式，如`demo.demo`；  
  任务参数`params`填对应任务类型**所应该接受的参数**。
  该方法并不会等待子任务执行，而仅仅是等待系统接受了调度信息便立即返回一个`Job`对象。

- `this.info(...messages)` / `this.warn(...messages)` / `this.error(...messages)`  
  日志打印方法，可接收任意个的参数，都会打印到日志里。  
  如果参数中有**原生**js对象，则自动会通过`JSON.stringify()`打印对象全部内容，而不需要用户手动调用。  
  如果参数中有Error对象，会智能打印出所有错误信息。
   
- `this.fail(code, message)` / `this.cancel(code, message)` / `this.crash(code, message)`  
  报错方法。这3种错误适用于不同的情况，产生的**后果也不同**。唯一相同的是它们都会**立刻中断**当前任务的执行。  
  `fail`方法用于在业务逻辑执行过程中遇到**预期**但不可处理的错误，例如目标网站临时不可访问，超时等。
  此错误会被系统记录，并自动被调度进入重试环节（如果对任务进行过相关配置）。  
  `cancel`方法用于业务逻辑发现**没有必要**继续执行时上报，系统会记录，但不会做任何重试。
  人工通过框架接口设置一个`Task`为`enabled: false`或者设置一个`Job`为`status: CANCELED`也会起到类似的效果。  
  `crash`方法用于业务逻辑发现不可挽回的错误从而**故意**报出。一个典型例子比如有些业务逻辑需要对任务输入的参数进行验证，
  发现输入了无法识别的值，从而造成任务执行没有意义。
  
除这些自带方法外，插件所为一种补充方法的机制，请见下一节。


## 4. 插件机制

上一节中系统自带的所有方法仅仅是最基础的一些功能，要想做一些爬网站等工作还缺失很多工具，比如网页解析器等。
与其业务开发者在文件头上去`require('puppeteer')`等自行引入第三方包，系统提供插件机制来替代解决此问题。好处在于：

- 可以自由开发一些不能简单通过`require`引入的功能，例如`data`这样我们内部用于存取数据的client；
- 可以针对框架使用情况进行一些定制，比如浏览器插件`browser`可以是基于`puppeteer`之上封装一些稳定性逻辑，如自动登录、内存控制等；
- 所有任务可以共享插件能力，并且支持层级化配置参数定义；
- 可以简单通过`this.<plugin>.<method>`来使用插件，业务逻辑更简洁，更聚焦于业务。

一个插件本质上是一个js模块，它被引入时表现为一个函数（构造器），只接收一个**js原生对象**作为参数，构造一个插件的实例。
构造一个实例时，参数对象中的字段定义取决于插件的设计，但是所有插件都有一个名为`singleton: true/false`的参数，
框架会根据此参数判断多个任务之间是共享一个插件实例还是分开创建不同实例。

插件不需要手动构造，在业务逻辑运行时，`this`上会根据配置已经准备好了插件实例，名称与插件名称一致，这样就可以随意调用插件的功能了。

以下是目前系统有的插件：

#### 4.1 `data`

用于操作*统一数据接口*的client端。爬虫爬到的数据都通过此插件进行输出；插件还可以用来查询目前已爬到的数据。
插件所提供的方法取决于*统一数据接口*提供的GraphQL的query/mutation接口，
详情可打开[接口网站](http://www.catatech.cn:3003/query)查看Docs。  

对于mutation接口，比如`UpdateUserInfo`，从doc中可以看出这个接口是用来更新/插入一个/多个达人的个人信息。
调用插件时，把前缀`Update`改为`update`，把所有接口需要的参数以一个js对象传入即可。以下是一些使用示例：

- 录入一个小红书达人12345  
  GraphQL的语法为`UpdateUserInfo(id: "12345", source: "xiaohongshu", nickname: "某人")`；  
  此插件的调用方法为`const resp = await this.data.updateUserInfo({id: "12345", source: "xiaohongshu", nickname: "某人"});`。
- 录入多个达人  
  GraphQL的语法为`UpdateUserInfo(inputs: [{id: "12345", ...}, {id: 23456, ...}])`；  
  此插件的调用方法为`const resp = await this.data.updateUserInfo({inputs: [{id: "12345", ...}, {id: 23456, ...}]});`。

对于query接口，比如`UserInfo`，这是一个查询达人信息的接口。
与上文类似，调用插件时，增加前缀`query`，把所有接口需要的参数以一个js对象传入。
由于GraphQL查询需要指定返回的字段，那么在插件调用时，增加第二个参数对象，把要返回的字段结构以一个js对象传入，对象中字段位置的值为`true`即可。示例如下：

- 查询所有昵称包含“张三”的达人并返回达人的个人主页链接  
  GraphQL的语法为`UserInfo(nickname_regex: ".*张三.*") {results {nickname, url}}`；  
  此插件的调用方法为`const resp = await this.data.queryUserInfo({nickname_regex: ".*张三.*"}, {results: {nickname: true, url: true}})`，
  此时返回的对象即形如`{"results": [{"nickname": "张三三", "url": "http://xxx"}, ...]}`。

#### 4.2 `proxy`

用于管理和对接代理池的插件。很多爬虫的业务场景



- `request`  
  用于向任意网站请求http。本质上是对`request`包进行了插件化的封装。  
  调用方法类似`const resp = await this.request({uri: 'http://www.baidu.com'})`。
  
- `browser`  
  用于在完全的Chrome浏览器环境下访问网站。详情请见`plugins/browser.js`。
