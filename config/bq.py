from os import getenv, path
from dotenv import load_dotenv

# Load variables from .env
basedir = path.abspath(path.dirname(__file__))
load_dotenv(path.join(basedir, "../.dotenv.dev"))

env: str = getenv("env")
K_SERVICE: str = getenv("K_SERVICE")

# GCP BQ Project
GCP_PROJECT_ID: str = "{project_id}"

# PUBSUB Project
PUBSUB_PROJECT_ID: str = "{project_id}"

# GCP Dataware house Dataset ID
GCP_BIGQUERY_DATASET_DWH_COMPANIES_DATA: str = 'dwh_profiles_data'

# GCP merged_entities Dataset ID
GCP_BIGQUERY_DATASET_MERGED_ENTITIES: str = 'profiles-merged'
