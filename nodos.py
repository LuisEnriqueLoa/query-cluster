from google.cloud import logging 
from datetime import datetime, timedelta, timezone
import json

client = logging.Client()
user="luisloa.7913@gmail.com"


now = datetime.now(timezone.utc).isoformat()
yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()

CLUSTER_LOCATION = 'us-central1-a'
CLUSTER_NAME = 'gke-cluster-demo'

filter_str = (
    f'resource.type="k8s_cluster" '
    f'AND resource.labels.location="{CLUSTER_LOCATION}" '
    f'AND resource.labels.cluster_name="{CLUSTER_NAME}" '
    f'AND log_id("cloudaudit.googleapis.com/activity") '
    f'AND protoPayload.methodName:"io.k8s.core.v1.nodes" '
    f'AND timestamp >= "{yesterday}" AND timestamp <= "{now}"'
)

#for entry in client.list_entries(filter_=filter_str, page_size=1):
    #print(json.dumps(entry.payload_json, indent=2))
    #print("")
    
for entry in client.list_entries(filter_=filter_str, page_size=1):
    payload = entry.payload
    if isinstance(payload, dict):
        type_node = payload.get("resource", {}).get("type", {})
        
        
    print(
        f"-Tipo: {type_node}"
    )