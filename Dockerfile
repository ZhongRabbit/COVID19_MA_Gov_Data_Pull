# krb5.conf & odbc.ini intentionally not included. Customize based on your own needs.

# Base official
FROM continuumio/miniconda3:latest

LABEL maintainer='Zhong Gao'

# Install packages for python ETL runtimes
RUN apt-get update && \
    conda config --add channels conda-forge && \
    conda install --yes \
    turbodbc \
    pyodbc \
    pyarrow=0.13* \
    pandas=0.24* \
    xlrd=1.2.0 \
    sqlalchemy=1.3* \
    psutil=5.6* \
    gcsfs=0.3.0 \
    google-cloud-sdk=255.0.0 \
    xgboost=0.90 \
    scikit-learn=0.22.* \
    google-cloud-storage=1.24.* \
    google-cloud-bigquery=1.23.* && \

    # Install the third party sqlalchemy-turbodbc bindings
    pip install sqlalchemy-turbodbc \
    google-cloud-bigquery \
    google-cloud-storage \
    toml==0.10.0 \
    webapp2==2.5.2

    # Install kerberos related dependencies
RUN export DEBIAN_FRONTEND=noninteractive && \
    apt-get -y install krb5-user libpam-krb5 libpam-ccreds less curl gpgv gnupg2 libssl-dev \
     apt-transport-https ca-certificates && \

    # apt-get update  libssl1.0.0 && \

    # Install MSFT drivers for SQLServer
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/9/prod.list > /etc/apt/sources.list.d/mssql-release.list &&\
    apt-get update  && \

    # Accept license agreement for MS odbc driver
    ACCEPT_EULA=Y apt-get -y install msodbcsql17 && \
    apt-get -y install unixodbc-dev && \

    # Remove default kerberos config
    rm /etc/krb5.conf

# Move our temporarily mounted config file to where it should be
# NOTE: docker build needs to be running from a directory that contains ./krb5.conf
COPY krb5.conf /etc/krb5.conf

# Move database config
COPY odbc.ini /etc/odbc.ini

# install google chrome
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
RUN sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'
RUN apt-get -y update
RUN apt-get install -y google-chrome-stable

# install chromedriver
RUN apt-get install -yqq unzip
RUN wget -O /tmp/chromedriver.zip http://chromedriver.storage.googleapis.com/`curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE`/chromedriver_linux64.zip
RUN unzip /tmp/chromedriver.zip chromedriver -d /usr/local/bin/

# set display port to avoid crash
ENV DISPLAY=:99

# upgrade pip & install selenium
RUN pip install --upgrade pip && \
    pip install selenium \
    bs4 \
    docx2csv \
    looker-sdk \
    pdfminer

# Install packages for dbt
RUN apt-get update && \
    apt update && \
    curl -sL https://deb.nodesource.com/setup_12.x | bash - && \
    apt-get install -y nodejs \
    nano \
    git-all && \
    npm install netlify-cli -g && \
    pip install 'dbt==0.14.0' && \
    apt-get install -y nano

COPY ./initialize.sh /

RUN chmod 777 /initialize.sh

ENTRYPOINT ["/initialize.sh"]
