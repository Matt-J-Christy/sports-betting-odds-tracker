#Deriving the latest base image
FROM python:3.9-slim
COPY requirements.in ./

SHELL ["/bin/bash", "-c"]

# install dependencies
RUN pip install -r ./requirements.in


#Labels as key value pair
LABEL Maintainer="matt_christy"


# Any working directory can be chosen as per choice like '/' or '/home' etc
WORKDIR /
ENV PYTHONPATH=/

#to COPY the remote file at working directory in container
COPY app/ ./app/
COPY cli.py run_cli.sh ./

#CMD instruction should be used to run the software
#contained by your image, along with any arguments.

CMD source run_cli.sh
