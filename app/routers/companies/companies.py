from fastapi import APIRouter, status
import utils
from data import Data
from publisher.publish import publish_messages
from app.routers.route import PushMessageRoute
from app.routers.schemas import PushMessageData
from big_query import BQ

router = APIRouter(route_class=PushMessageRoute)

@router.post("/companies", status_code=status.HTTP_201_CREATED)
def companies(push_message_data: PushMessageData):

    """Merge 2 entities."""

    from_id = push_message_data.from_id
    to_id = push_message_data.to_id
    env = push_message_data.env

    data = Data()
    bq = BQ()

    date = utils.get_date()
    date_time = utils.get_date_time()

    check_profile_exist = bq.check_profile_exist(
            from_id=from_id, to_id=to_id, date=date
    )

    if not check_profile_exist:
        add_profile = bq.add_profile_data(
            from_id=from_id, to_id=to_id, date=date, date_time=date_time, status="to do", type="companies", env=env, table="duplicates"
        )

        if not add_profile:
            return f"Encountered errors while adding company profile data: {from_id}, {to_id}"

    messages = []

    """This method gets the profile from big query database and publish the data through another service /v2/entity/update route"""

    profile = data.get_profile(
        from_id=from_id,
        to_id=to_id,
        entity_type="data",
        env=env
    )

    if profile:
        messages.append(profile)

    """This method get the tags data which can be imported by another service /v2/startup/update/tags route"""

    tags = data.get_tags(
        from_id=from_id,
        to_id=to_id,
        entity_type="data",
    )

    if tags:
        messages.append(tags)

    """This method gets the investments data which can be imported by another service /v2/company/investment/add route"""

    investments = data.get_investments(
        from_id=from_id,
        to_id=to_id,
        entity_type="data",
    )

    if investments:
        messages.append(investments)

    """This method gets the teams from big query database and publish the data through another service /v2/user/update route"""

    teams = data.get_teams(
        from_id=from_id,
        to_id=to_id,
        entity_type="data",
    )

    if teams:
        messages.append(teams)

    """This method gets the funds from big query database and publish the data through another service /v2/entity/update/fund/add route"""

    funds = data.get_funds(
        from_id=from_id,
        to_id=to_id,
        entity_type="data",
    )

    if funds:
        messages.append(funds)

    """This method gets the news from big query database and publish the data through another service /v2/entity/add/news route"""

    news = data.get_news(
        from_id=from_id,
        to_id=to_id,
        entity_type="data",
    )

    if news:
        messages.append(news)

    """This method gets the companies of rounds from big query database and publish the data through another service /v2/company/funding/update route"""

    investor_in_rounds = data.get_investor_in_rounds(
        from_id=from_id,
        to_id=to_id,
        entity_type="data",
    )

    if investor_in_rounds:
        messages.append(investor_in_rounds)

    """This method gets the analytics from big query database and transfer the data through another service /v2/entity/update/chart/update route"""

    analytics = data.get_analytics(
        from_id=from_id,
        to_id=to_id,
        entity_type="data",
    )

    if analytics:
        messages.append(analytics)

    """This method gets the rounds from big query database and publish the data through another service /v2/company/funding/update route"""

    rounds = data.get_rounds(
        from_id=from_id,
        to_id=to_id,
        entity_type="data",
    )

    if rounds:
        messages.append(rounds)

    """This method gets the address from big query database and publish the data through another service /v2/entity/update/location/add route"""

    location = data.get_location(
        from_id=from_id,
        to_id=to_id,
        entity_type="data",
    )

    if location:
        messages.append(location)

    """This method gets the lists from big query database and publish the data through another service /v2/entity/add-to-list route"""

    lists = data.get_lists(
        from_id=from_id,
        to_id=to_id,
        entity_type="data",
    )

    if lists:
        messages.append(lists)

    jobs = data.get_jobs(
        from_id=from_id,
        to_id=to_id,
        entity_type="data",
    )

    if jobs:
        messages.append(jobs)

    """This method publish the data through another service"""

    publish_messages(request_bodies=messages, env=env)

    update_status = bq.add_profile_data(
            from_id=from_id, to_id=to_id, date=date, date_time=date_time, status="done", type="companies", env=env, table="profile_merged"
    )

    messages.append("company profile is in merging queue")

    if update_status:
        return messages
    else:
        return f"error in updating status of company profile"
