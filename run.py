# ONLY FOR DEVELOPMENT PURPOSE.
import os
from os.path import dirname, join
from dotenv import dotenv_values, load_dotenv

env = os.getenv("env", default="dev")

dotenv_path = join(dirname(__file__), f".dotenv.{env}")
load_dotenv(dotenv_path)

if __name__ == "__main__":
    import uvicorn

    env_vars_string = "\n".join(
        [f"{k}: {v}" for k, v in dotenv_values(dotenv_path).items()]
    )
    print(
        "Starting development server with set environment variables: \n",
        env_vars_string,
    )

    uvicorn.run("app.main:app", port=8001, reload=True)
