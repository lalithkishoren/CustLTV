from diagrams import Diagram, Cluster, Edge
from diagrams.custom import Custom
from diagrams.onprem.database import Oracle
from diagrams.azure.analytics import DataFactories, AzureDatabricks, LogAnalyticsWorkspaces
from diagrams.azure.storage import DataLakeStorage
from diagrams.onprem.analytics import Powerbi
from diagrams.azure.integration import DataCatalog
from diagrams.azure.security import KeyVaults
from diagrams.azure.identity import AzureActiveDirectory
from diagrams.azure.devops import AzureDevops

# Professional Styling Configuration
graph_attr = {
    "fontname": "Arial",
    "fontsize": "14",
    "fontcolor": "#1a1a1a",
    "bgcolor": "white",
    "pad": "0.25",
    "nodesep": "0.35",
    "ranksep": "0.6",
    "splines": "ortho",
    "labelloc": "t",
    "labeljust": "c",
    "dpi": "200",
    "compound": "true",
    "concentrate": "false",
    "newrank": "true"
}

node_attr = {
    "fontname": "Arial",
    "fontsize": "9",
    "fontcolor": "#333333",
    "imagescale": "true",
    "labelloc": "b",
    "width": "0.8",
    "height": "0.8"
}

edge_attr = {
    "fontname": "Arial",
    "fontsize": "9",
    "fontcolor": "#333333",
    "arrowsize": "0.8",
    "penwidth": "1.5",
    "minlen": "2"
}

cluster_sources = {"bgcolor": "#E8F5E9", "pencolor": "#2E7D32", "penwidth": "2", "style": "rounded", "fontname": "Arial Bold", "fontsize": "11", "fontcolor": "#1B5E20"}
cluster_ingestion = {"bgcolor": "#E3F2FD", "pencolor": "#1565C0", "penwidth": "2", "style": "rounded", "fontname": "Arial Bold", "fontsize": "11", "fontcolor": "#0D47A1"}
cluster_datalake = {"bgcolor": "#FAFAFA", "pencolor": "#90A4AE", "penwidth": "2", "style": "rounded", "fontname": "Arial Bold", "fontsize": "12", "fontcolor": "#546E7A"}
cluster_bronze = {"bgcolor": "#FFF8E1", "pencolor": "#FF8F00", "penwidth": "2", "style": "rounded", "fontname": "Arial Bold", "fontsize": "10", "fontcolor": "#E65100"}
cluster_silver = {"bgcolor": "#ECEFF1", "pencolor": "#546E7A", "penwidth": "2", "style": "rounded", "fontname": "Arial Bold", "fontsize": "10", "fontcolor": "#37474F"}
cluster_gold = {"bgcolor": "#FFFDE7", "pencolor": "#F9A825", "penwidth": "2", "style": "rounded", "fontname": "Arial Bold", "fontsize": "10", "fontcolor": "#F57F17"}
cluster_processing = {"bgcolor": "#F3E5F5", "pencolor": "#7B1FA2", "penwidth": "2", "style": "rounded", "fontname": "Arial Bold", "fontsize": "11", "fontcolor": "#6A1B9A"}
cluster_consumption = {"bgcolor": "#E8EAF6", "pencolor": "#303F9F", "penwidth": "2", "style": "rounded", "fontname": "Arial Bold", "fontsize": "11", "fontcolor": "#1A237E"}
cluster_platform = {"bgcolor": "#FAFAFA", "pencolor": "#9E9E9E", "penwidth": "1", "style": "rounded,dashed", "fontname": "Arial", "fontsize": "10", "fontcolor": "#616161"}

with Diagram(
    "Customer Lifetime Value (CLV) Analytics - Data Platform",
    show=False,
    filename="./clv_analytics_architecture",
    direction="LR",
    outformat=["png", "dot"],
    graph_attr=graph_attr,
    node_attr=node_attr,
    edge_attr=edge_attr
):

    # Custom Icons defined before clusters
    marketing_api = Custom("Marketing Platform", "./icons/generic_placeholder.png")

    # Stage 1: Data Sources
    with Cluster("Data Sources", graph_attr=cluster_sources):
        erp = Oracle("Oracle ERP\n(Orders/Lines)")
        crm = Oracle("Oracle CRM\n(Customers)")
        marketing = marketing_api

    # Stage 2: Ingestion
    with Cluster("Ingestion", graph_attr=cluster_ingestion):
        autoloader = AzureDatabricks("Auto Loader\n(Continuous CDC)")
        adf = DataFactories("Data Factory\n(Daily Batch)")

    # Stage 3 & 4: Storage & Processing (Medallion Architecture)
    with Cluster("Data Lakehouse (ADLS Gen2 & Databricks)", graph_attr=cluster_datalake):
        
        with Cluster("Bronze Layer", graph_attr=cluster_bronze):
            bronze_storage = DataLakeStorage("Raw Data\n(Append-Only)")
            
        with Cluster("Silver Layer", graph_attr=cluster_silver):
            dlt_silver = AzureDatabricks("DLT Pipelines\n(Cleanse & Validate)")
            silver_storage = DataLakeStorage("Conformed Data\n(MERGE/Upsert)")
            
        with Cluster("Gold Layer", graph_attr=cluster_gold):
            dlt_gold = AzureDatabricks("DLT Pipelines\n(Aggregations)")
            gold_storage = DataLakeStorage("CLV Models\n(Materialized Views)")

    # Stage 5: Consumption
    with Cluster("Consumption", graph_attr=cluster_consumption):
        powerbi = Powerbi("Power BI\n(Executive Dashboards)")

    # Stage 6: Platform & Foundation (Unconnected Context)
    with Cluster("Platform & Foundation Services", graph_attr=cluster_platform):
        uc = DataCatalog("Unity Catalog\n(Governance & Lineage)")
        kv = KeyVaults("Key Vault\n(Secrets Management)")
        monitor = LogAnalyticsWorkspaces("Azure Monitor\n(Logging & Alerting)")
        entra = AzureActiveDirectory("Entra ID\n(RBAC & Identity)")
        devops = AzureDevops("Azure DevOps\n(CI/CD & IaC)")

    # Data Flow Connections
    erp >> Edge(label="Debezium CDC", color="#2962FF") >> autoloader
    crm >> Edge(label="Debezium CDC", color="#2962FF") >> autoloader
    marketing >> Edge(label="API Extract", color="#2962FF") >> adf

    autoloader >> Edge(label="Stream Write", color="#2962FF") >> bronze_storage
    adf >> Edge(label="Batch Copy", color="#2962FF") >> bronze_storage

    bronze_storage >> Edge(label="Read Stream", color="#2962FF") >> dlt_silver
    dlt_silver >> Edge(label="Apply Changes", color="#2962FF") >> silver_storage

    silver_storage >> Edge(label="Read Stream", color="#2962FF") >> dlt_gold
    dlt_gold >> Edge(label="Calculate KPIs", color="#2962FF") >> gold_storage

    gold_storage >> Edge(label="DirectQuery/Import", color="#2962FF") >> powerbi