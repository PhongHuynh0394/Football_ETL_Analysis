FROM jupyter/base-notebook:python-3.8

# Set working direction
WORKDIR /home/jovyan

COPY requirements.txt requirements.txt

#RUN pip install -r requirement.txt
RUN pip install --upgrade pip && pip install -r requirements.txt