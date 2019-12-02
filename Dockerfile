FROM python:3.6
RUN mkdir -p /home/backend
WORKDIR /home/backend
COPY . /home/backend
RUN python3 -m pip install -r requirements.txt
RUN export GOOGLE_APPLICATION_CREDENTIALS=""
CMD python3 process_bigquery_kubernetes.py



