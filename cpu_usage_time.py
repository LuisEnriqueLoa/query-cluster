import json
from google.cloud import monitoring_v3
from datetime import datetime, timedelta, timezone
from google.protobuf.json_format import MessageToDict
import pandas as pd


def fetch_cpu_request_utilization(project_id: str, minutes: int = 10):
    client = monitoring_v3.MetricServiceClient()
    project_name = f"projects/{project_id}"

    interval = monitoring_v3.TimeInterval({
        "end_time": datetime.now(timezone.utc),
        "start_time": datetime.now(timezone.utc) - timedelta(minutes=minutes)
    })

    filter_str = (
        'metric.type="kubernetes.io/container/cpu/request_utilization" '
        'AND resource.type="k8s_container" '
    )

    results = client.list_time_series(request={
        "name": project_name,
        "filter": filter_str,
        "interval": interval,
        "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL
    })

    # Convert each TimeSeries to a dictionary
    #series_list = []
    #for series in results:
    #    series_dict = MessageToDict(series._pb)  # Access protobuf object
    #    series_list.append(series_dict)

    return results

def build_dataframe(time_series):
    import pandas as pd

    rows = []
    for series in time_series:
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

    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df

def analyze_cpu_utilization(df, threshold=0.1):
    # Opción 1: Promedio de utilización por contenedor
    avg_by_container = df.groupby("container")["utilization"].mean().sort_values(ascending=False)

    # Opción 3: Contenedores con puntos por debajo del umbral
    underutilized = df[df["utilization"] < threshold]
    underutilized_counts = underutilized.groupby("container").size().sort_values(ascending=False)

    return avg_by_container, underutilized_counts


if __name__ == "__main__":
    project_id = "brave-airship-473500-u8"
    results = fetch_cpu_request_utilization(project_id, minutes=10)
    
    df = build_dataframe(results)
    avg, underutilized = analyze_cpu_utilization(df)

    print("--Promedio de utilización por contenedor:")
    print(avg)

    print("\n- Contenedores con puntos por debajo del 10%:")
    print(underutilized)


    






