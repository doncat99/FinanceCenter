# Financial Center：Gathering open financial data and store in Relational Database

## Installation guide

The FinDy installation consists of setting up the following components:

1.  Packages / Dependencies
2.  Database
3.  Redis

## 1. Packages / Dependencies

Command line tools

```
xcode-select --install #xcode command line tools
```

Homebrew

```
ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
brew install git cmake pkg-config openssl
brew link openssl --force
```


## 2. Database

FinDy recommends using a PostgreSQL database. But you can use MySQL too, see [MySQL setup guide](database_mysql.md).

```
brew install postgresql
ln -sfv /usr/local/opt/postgresql/*.plist ~/Library/LaunchAgents
launchctl load ~/Library/LaunchAgents/homebrew.mxcl.postgresql.plist
```

Login to PostgreSQL

```
psql -d postgres
```

Create a user for FinDy.

```
CREATE USER xxx;
```

Create the FinDy production database & grant all privileges on database

```
CREATE DATABASE findy OWNER xxx;
```

Quit the database session

```
\q
```

Try connecting to the new database with the new user

```
sudo -u git -H psql -d findy
```

## 3. Redis

```
brew install redis
ln -sfv /usr/local/opt/redis/*.plist ~/Library/LaunchAgents
```

Redis config is located in `/usr/local/etc/redis.conf`. Make a copy:

```
cp /usr/local/etc/redis.conf /usr/local/etc/redis.conf.orig
```

Disable Redis listening on TCP by setting 'port' to 0

```
sed 's/^port .*/port 0/' /usr/local/etc/redis.conf.orig | sudo tee /usr/local/etc/redis.conf
```

Edit file (`nano /usr/local/etc/redis.conf`) and uncomment:

```
unixsocket /tmp/redis.sock
unixsocketperm 777
```

Start Redis

```
launchctl load ~/Library/LaunchAgents/homebrew.mxcl.redis.plist
```


### Configure FinDy DB Settings




### More

You can find more tips in [official documentation](https://github.com/gitlabhq/gitlabhq/blob/8-0-stable/doc/install/installation.md#advanced-setup-tips).

## Todo




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