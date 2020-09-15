ZVT 源項目地址：https://github.com/zvtvz/zvt

本项目改进点包括：
* 增加postgresql数据库支持，允许远程数据访问，实现微服务器数据爬虫与GPU模型训练工作站逻辑分离。
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

为管控界面信息输出，需要对jqdatasdk项目进行部分修改，详见：https://github.com/doncat99/jqdatasdk
为加速数据下载体验，需要对yfinance项目进行部分修改，详见：https://github.com/doncat99/yfinance

python3.8在多进程处理上有较大改动与提升，建议使用python 3.8版本运行本项目。


