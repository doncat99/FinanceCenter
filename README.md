Project goal：Fetching open finance data from internet, and store in Relational Database

ZVT Original Project link: https://github.com/zvtvz/zvt

Improvement include:
* Switch Sql to Postgresql, which allow remote database access support, separate fetching and analysis models to two individal project(see [FinanceAnalysis](https://github.com/doncat99/FinanceAnalysis) project for more detail).
* Centralized http request interface，add Session and retry logic (Rxpy may be involved to rewrite request model pretty soon).
* multiprocessing is involved to boost up request speed.
* request progress display support.
* ~~auto joinquant account switching~~ (banned)
* log to console -> log to file.
* US stock market data support.
* global timezone support.
* Bugfix: evaluate_start_end_size_timestamps() logic error, produce redundant requests.
* Bugfix: data over 800w per sheet, memory cache overflow, cause database slow response.
* tiny bug fix.

For better performance, required customized jqdatasdk. see: [jqdatasdk](https://github.com/doncat99/jqdatasdk)  
For better performance, required customized yfinance. see:[yfinance](https://github.com/doncat99/yfinance)

It's suggest to run the project upon python3.8

------------------------------------------------------------------------

本项目改进点包括：
* 增加postgresql数据库支持，允许远程数据访问，实现微服务器数据爬虫与GPU模型训练工作站逻辑分离(见[FinanceAnalysis](https://github.com/doncat99/FinanceAnalysis)项目)。
* 统一http访问入口，增加Session与请求重试逻辑（后续可通过RxPy实现响应式编程的异步模式）。
* 增加多进程数据请求逻辑，允许数据并发请求，加快数据获取。
* 增加数据爬取进度展示。
* ~~增加joinquant账号切换功能~~ (被封号，代码保留但注释了，Todo：Mac地址替换和ip池)。
* 增加log信息从控制台输出改为写文件记录。
* 增加美股数据拉取功能（分库分表）。
* 增加不同区域股市时区功能。
* 修正增量数据请求的计算逻辑（原作者的 evaluate_start_end_size_timestamps() 有较大逻辑漏洞，导致重复请求较多数据）。
* 修正800w单表数据下query.all()的数据库内存缓存溢出改写文件问题（增加window_query功能）。
* 修正一些逻辑漏洞。  

为管控界面信息输出，需要对jqdatasdk项目进行部分修改，详见：[jqdatasdk](https://github.com/doncat99/jqdatasdk)  
为加速数据下载体验，需要对yfinance项目进行部分修改，详见：[yfinance](https://github.com/doncat99/yfinance)

python3.8在多进程处理上有较大改动与提升，建议使用python 3.8版本运行本项目。
