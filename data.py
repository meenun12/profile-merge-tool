from big_query import BQ
import utils
from pubsub import notify_slack
from typing import List, Dict, Union
import constants
import collections
from publisher.publish import publish_messages
class Data:

    def __init__(self):
        self.bq = BQ()

    def get_investments(self,
        from_id: int,
        to_id: int,
        entity_type: str
    ) -> dict:

        """This method create the csv file of the investments which can be imported by another service /v2/company/investment/add route

        Args:
            from_id (int): contains the {company} id of profile
            to_id (str): job id need to be pass during data import for firestore
            from_investor (str): contains the {company} id of profile
            to_investor (str): job id need to be pass during data import for firestore

        """
        data = {}

        job_id = utils.job_id(entity_type=entity_type, data_type="investments")
        base = {"job_type": "add_new_investment", "job_id": job_id}

        investments = self.bq.get_investments_data(
            from_id=from_id, to_id=to_id
        )

        investors_without_funding = self.bq.get_investors_without_funding_data(
            from_id=from_id, to_id=to_id
        )

        investments.update(investors_without_funding)

        if not investments:
            return data

        data["add-investments"] = [{**base, **v} for k, v in investments.items() if v != None]

        return data

    def get_news(self,
        from_id: int,
        to_id: int,
        entity_type: str
    ) -> dict:

        """This method gets the news from big query database and publish the data through another service /v2/entity/add/news route
        Args:
            from_id (int): contains the {company} id of profile
            to_id (str): job id need to be pass during data import for firestore
            from_investor (str): contains the {company} id of profile
            to_investor (str): job id need to be pass during data import for firestore

        """

        data = {}

        job_id = utils.job_id(entity_type=entity_type, data_type="news")
        base = {"job_type": "add_news_links", "job_id": job_id, "{company}_id": to_id}

        news = self.bq.get_news_data(
            from_id=from_id, to_id=to_id
        )

        if not news:
            return data

        data["add-news-links"] = [{**base, "payload": {k:v for k,v in news[news_id].items() if v not in [None, ""]}} for news_id in news]

        return data

    def get_teams(self,
        from_id: int,
        to_id: int,
        entity_type: str
    ) -> dict:

        """This method gets the teams from big query database and publish the data through another service /v2/user/update route

        Args:
            from_id (int): contains the {company} id of profile
            to_id (str): job id need to be pass during data import for firestore
            from_investor (str): contains the {company} id of profile
            to_investor (str): job id need to be pass during data import for firestore

        """

        data = {}

        teams = self.bq.get_team_data(
            from_id=from_id, to_id=to_id, entity_type=entity_type
        )

        if not teams:
            return data

        data["update-user"] = [{k:v for k,v in teams[team].items()} for team in teams]
        return data

