FROM python:3.8

WORKDIR /work/

COPY ./requirements.txt  /work/

#RUN pip install git+https://github.com/doncat99/yfinance.git \
RUN    pip install -i https://pypi.douban.com/simple -r requirements.txt 

CMD python /work/run.py


