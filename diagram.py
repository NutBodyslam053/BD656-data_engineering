from diagrams import Diagram, Cluster
from diagrams.gcp.api import APIGateway
from diagrams.gcp.storage import GCS
from diagrams.gcp.analytics import BigQuery
from diagrams.onprem.analytics import Metabase
from diagrams.custom import Custom


diagram_attr = {
    # "margin": "-1, -1.5",
}

cluster_attr = {
    "bgcolor": "white",
}


with Diagram("Data Ingestion Pipeline", show=False, graph_attr=diagram_attr, filename="./data_pipeline", outformat="png"):
    with Cluster("Extract", graph_attr=cluster_attr):
        reddit_api = APIGateway("Reddit API")

    with Cluster("Load", graph_attr=cluster_attr):
        gcs_raw = GCS("Google Cloud Storage\n<Raw Bucket>")

    with Cluster("Transform", graph_attr=cluster_attr):
        gcs_processed = GCS("Google Cloud Storage\n<Processed Bucket>")
        bq_table = BigQuery("BigQuery\n<Table>")
        dbt = Custom("dbt", "./images/icons/dbt_icon.png")
        bq_view = BigQuery("BigQuery\n<View>")

    with Cluster("Visualize", graph_attr=cluster_attr):
        metabase = Metabase("Metabase")

    reddit_api >> gcs_raw >> gcs_processed >> bq_table >> dbt >> bq_view >> metabase
