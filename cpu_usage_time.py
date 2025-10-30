import json
from google.cloud import monitoring_v3
from datetime import datetime, timedelta, timezone
from google.protobuf.json_format import MessageToDict
import pandas as pd


def fetch_cpu_request_utilization(project_id: str, minutes: int = 10):
    # Instacia cliente para API Monitoring
    client = monitoring_v3.MetricServiceClient()
    
    # Formato requerido para la API
    project_name = f"projects/{project_id}"

    interval = monitoring_v3.TimeInterval({
        "end_time": datetime.now(timezone.utc),
        "start_time": datetime.now(timezone.utc) - timedelta(minutes=minutes)
    })
    #-----------------------start_time----------------------|end_time
    #                             -------------------------->

    # Filtro para obtener la utilizacion de CPU por contenedor
    filter_str = (
        'metric.type="kubernetes.io/container/cpu/request_utilization" '
        'AND resource.type="k8s_container" '
    )

    # Solicitud a la API con los parametro definidos
    results = client.list_time_series(request={
        "name": project_name,
        "filter": filter_str,
        "interval": interval,
        "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL
    })

    # Datos en bruto
    #for series in results:
    #    for point in series.points:
    #        container = series.resource.labels.get("container_name")
    #        pod = series.resource.labels.get("pod_name")
    #        print(container, pod, point.value.double_value)

    #print(type(results))
    
    # Retorna un google.cloud.monitoring_v3.services.metric_service.pagers.ListTimeSeriesPager
    return results

# Convierte ListTimeSeries a DataFrame
def build_dataframe(list_time_series):
    import pandas as pd

    # Genera una list de dics con los valores de TimeSeries requeridos
    rows = []
    for series in list_time_series:
        resource = series.resource.labels
        for point in series.points:
            rows.append({
                "cluster": resource.get("cluster_name"),
                "namespace": resource.get("namespace_name"),
                "container": resource.get("container_name"),
                "pod": resource.get("pod_name"),
                "timestamp": point.interval.end_time.strftime("%Y-%m-%d %H:%M:%S"),
                "utilization": point.value.double_value
            })

    # Covierte a DataFrame
    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df

def analyze_cpu_utilization(df, threshold=0.1):

    avg_by_container = df.groupby("container")["utilization"].mean().sort_values(ascending=False)

    underutilized = df[df["utilization"] < threshold]
    underutilized_counts = (underutilized.groupby("container").size().sort_values(ascending=False))

    return avg_by_container, underutilized_counts


if __name__ == "__main__":
    project_id = "brave-airship-473500-u8"
    results = fetch_cpu_request_utilization(project_id, minutes=10)
    
    df = build_dataframe(results)
    avg, underutilized = analyze_cpu_utilization(df)

    print("--Promedio de utilización por contenedor:")
    print(avg)

    print("\n- Contenedores con puntos por debajo del 10%:")
    print(f'{underutilized}%')


    






