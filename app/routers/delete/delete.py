from fastapi import APIRouter, status
from publisher.publish import publish_messages
from app.routers.route import PushMessageRoute
from .schemas import PushMessageData
import utils
from big_query import BQ

router = APIRouter(route_class=PushMessageRoute)


@router.post("/delete", status_code=status.HTTP_201_CREATED)
def delete(push_message_data: PushMessageData):

    """
    This route deletes the merged profiles
    :param from id: int, Take the id of entity which needs to be removed
    :param env: str, Take the env either dev or prod
    """

    from_id = push_message_data.from_id
    env = push_message_data.env
    bq = BQ()
    date = utils.get_date()
    date_time = utils.get_date_time()

    data = {}

    job_id = utils.job_id(entity_type="data", data_type="delete")

    data["delete-entity"] = [{"job_type": "delete_entity", "job_id": job_id, "{company}_id": from_id}]

    publish_messages(request_bodies=[data], env=env)

    """
     This method update the removed status in this bq table `spheric-hawk-262313.merged_entities.duplicates`
    :param from id: int, Take the id of entity which needs to be removed
    :param date: str, Take the date
    :status:str, removed
    """

    update_status = bq.update_removed_status(
        from_id=from_id, date=date, date_time=date_time, env=env, status="removed"
    )

    if update_status:
        return f"profile is in removal queue"
    else:
        return f"error in updating status of profiles"
