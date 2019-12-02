## Setting the environment
```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Setting the credentials for BigQuery
In the command line:
```
export GOOGLE_APPLICATION_CREDENTIALS="[PATH]"
```
If this does not work, then specify this inside the script:
```
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="[PATH]"
```

## Deployment
Start Docker:
```
sudo service docker start
sudo docker build -f Dockerfile -t <name_of_image> .
sudo docker tag <name_of_image> gcr.io/<PROJECT NAME>/<name_of_image_in_GCP> 
sudo docker push gcr.io/<PROJECT NAME>/<name_of_image_in_GCP>
gcloud container clusters create <cluster_name> --zone=us-central1-a --machine-type=g1-small --num-nodes=1 --enable-autoscaling --max-nodes=3 --min-nodes=1

```

