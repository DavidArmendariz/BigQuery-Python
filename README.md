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
