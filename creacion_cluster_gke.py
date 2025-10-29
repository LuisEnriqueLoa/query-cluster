from google.cloud import logging 
from datetime import datetime, timedelta, timezone
import json


client = logging.Client()


now = datetime.now(timezone.utc).isoformat()
yesterday = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()

filter_str = (
    'resource.type="gke_cluster" '
    'AND logName="projects/brave-airship-473500-u8/logs/cloudaudit.googleapis.com%2Factivity" '
    'AND protoPayload.methodName="google.container.v1.ClusterManager.CreateCluster" '
)


for entry in client.list_entries(filter_=filter_str):
    print(json.dumps(entry.payload_json, indent=2))
    print("")  


for entry in client.list_entries(filter_=filter_str, page_size=3):
    payload = entry.payload
    if isinstance(payload, dict):
        cluster = payload.get("request", {}).get("cluster", {})
        node_pool = cluster.get("nodePools", [{}])[0]
        scopes = node_pool.get("config", {}).get("oauthScopes", [])
        email = payload.get("authenticationInfo", {}).get("principalEmail", "")
        cluster_name = cluster.get("name", "")
        zone = payload.get("resourceLocation", {}).get("currentLocations", [""])[0]
        method = payload.get("methodName", "")
        status = payload.get("response", {}).get("status", "")
        ip_aliasing = cluster.get("ipAllocationPolicy", {}).get("useIpAliases", False)
        node_count = int(node_pool.get("initialNodeCount", 0))
        scopes_clean = ", ".join([s.split("/")[-1] for s in scopes])

        print(
            f"- Usuario: {email} \n "
            f"- Clúster: {cluster_name} \n "
            f"- Zona: {zone} \n "
            f"- Método: {method} \n "
            f"- Estado: {status} \n "
            f"- IP aliasing: {'activado' if ip_aliasing else 'desactivado'} \n "
            f"- Nodos iniciales: {node_count} \n "
            f"- Scopes de acceso: {scopes_clean} \n"
        )






