# Profile merge tool

This Cloud Run service is designed to help users merge two profiles into a single profile on our platform. When a user initiates a merge request, the service retrieves data from our data warehouse using the provided IDs. It then uses a combination of BigQuery SQL queries and Python code to identify the differences between the two profiles, such as variations in industries, analytics, and funds etc.

Once the service has identified the differences between the profiles, it sends a message to a PubSub topic, which triggers another service responsible for importing the data into our platform. This allows us to quickly and seamlessly update the company's profile with the new information, providing a more accurate and up-to-date representation of their skills and experience.

Overall, our Cloud Run service provides an efficient and effective solution for merging profiles on our platform, while leveraging the power of BigQuery and PubSub to ensure that the process is fast, reliable, and scalable.

Endpoints:
* `/companies`: move missing data from "company to move" to "company to keep"

	```json
	{
		"from_id": 12345,
		"to_id": 56789
	}
	```
 
Please check the logs on cloud run to make sure data has imported

## Development

### Installing requirements

1. [Install poetry](https://python-poetry.org/docs/)
1. Install requirements: `poetry install`
1. Enable environment: `poetry shell`


### Setting up environment

We use [python-dotenv](https://pypi.org/project/python-dotenv/) for development to define the environment variables.
Before starting Copy paste [.dotenv.example](.dotenv.example) to `.dotenv.dev` and edit it defining any needed env vars.

### How to set up service accounts

1.Create the SA that will invoke `profil-merge-tool`:

# SA for CLoud Run

1. gcloud iam service-accounts create profile-merge-tool \
      --project={project_name} \
      --display-name="profile-merge-tool" \
      --description="Used by profile-merge-tool Cloud Run to access needed resources."

2. Given the permissions to the SA to access dataset `profile-merge-tool` in edit mode and dataset `dwh_{company}_data` in read-only mode (done via front-end).

# PubSub

1. Create the SA that will invoke `profile-merge-tool`:
```shell
gcloud iam service-accounts create profile-merge-tool-invoker \
  --project={project_name} \
  --display-name="profile-merge-tool-invoker" \
  --description="Invoker of profile-merge-tool Cloud Run, used by the topic profile-merge."
```

2. Give the SA role to invoke `profile-merge-tool`:
```shell
 gcloud run services add-iam-policy-binding profile-merge-tool \    
    --member='serviceAccount:profile-merge-tool-invoker@{project_name}.iam.gserviceaccount.com' \
    --role='roles/run.invoker' \
    --project={project_name} \
    --region=europe-west4
```

3. Create the topic:
```shell
  gcloud pubsub topics create profile-merge \
    --labels=service=profile-merge-tool
```

4. Add a push subscription to the topic that uses the SA above to authenticate:
```shell
gcloud pubsub subscriptions create profile-merge-sub \
  --topic=profile-merge \
  --project={project_name} \
  --ack-deadline=600 \
  --expiration-period=never \
  --message-retention-duration=30m \
  --push-endpoint={endpoint} \  
  --push-auth-service-account=profile-merge-tool-invoker@{project_name}.iam.gserviceaccount.com \
  --labels=service=profile-merge-tool
```

### Run the server

`python run.py`

### Test the server

`curl -X 'POST' \
  'http://127.0.0.1:8001/companies' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "from_id": int,
  "to_id": int
}`

