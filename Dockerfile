FROM python:3.11

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

WORKDIR /work/

COPY requirements.txt /work/
COPY requirements-dashboard.txt /work/

# install python dependencies
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

COPY . /work/

# gunicorn
CMD ["gunicorn", "--config", "gunicorn-cfg.py", "run:app"]
