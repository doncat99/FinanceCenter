# Project goal：Fetching open financing data and store in Relational Database

## ZVT Original Project link: https://github.com/zvtvz/zvt


## Improvement (01 Mar 2021):
* database session decoupling
* entity provider removed
* database querying speed boost up
* asyncio logic framework support (implementations requires rewrite request logic)
* multiprocessing logic rewrite, use processes to replace pool
* add proxy pool support
* add baostock thrid party source support


## Improvement (16 Sep 2020):
* Switch Sql to Postgresql, which allow remote database access support, separate fetching and analysis models to two individal project(see [FinanceAnalysis](https://github.com/doncat99/FinanceAnalysis) project for more detail).
* Centralized http request interface，add Session and retry logic (Rxpy may be involved to rewrite request model pretty soon).
* multiprocessing is involved to boost up request speed.
* request progress display support.
* ~~auto joinquant account switching~~ (banned)
* log to console -> log to file.
* US stock market data support.
* global timezone support.
* tiny bug fix.


For proxy use, required proxy_pool support. see: [proxy_pool](https://github.com/doncat99/proxy_pool)  
For better performance, required customized jqdatasdk. see: [jqdatasdk](https://github.com/doncat99/jqdatasdk)  
For better performance, required customized yfinance. see: [yfinance](https://github.com/doncat99/yfinance)

It's suggest to run the project upon python 3.8

# 使用Docker

## 容器化编排执行
```
docker-compose stop; docker-compose rm -f ; docker-compose build --no-cache
docker-compose up -d
```

查看日志
```
docker-compose logs -f 
```