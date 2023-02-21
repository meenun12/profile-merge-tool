from fastapi import FastAPI, status
from app.routers.companies import companies
from app.routers.delete import delete
from app.routers.universities import universities
from app.routers.people import people

app = FastAPI()

@app.get("/", status_code=status.HTTP_200_OK, tags=["App Info"])
def get_app_info():
    """Print info on app."""
    return {
        "info": "Profile merging tool",
        "app_version": "0.1",
    }

app.include_router(companies.router, tags=["PubSub"])
app.include_router(delete.router, tags=["PubSub"])



