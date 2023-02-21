from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from config.bq import *
from typing import List, Dict, Union
import utils


class BQ:
    def __init__(self):

        self.client = bigquery.Client(GCP_PROJECT_ID)
        self.dataset_company_data = self.client.dataset(
            GCP_BIGQUERY_DATASET_DWH_COMPANY_DATA
        )
        self.dataset_merged_entities = self.client.dataset(
            GCP_BIGQUERY_DATASET_MERGED_ENTITIES
        )
        self.basedir = path.abspath(path.dirname(__file__))

        try:
            self.client.get_dataset(self.dataset_company_data)
        except NotFound:
            print(f"company Dataset {self.dataset_company_data} Not Found.")

        try:
            self.client.get_dataset(self.dataset_merged_entities)
        except NotFound:
            print(f"company Dataset {self.dataset_merged_entities} Not Found.")

    def get_from_people_university_data(self, from_id: int, to_id: int):

        from_people_university_data: str = """select bu.bobject_user_id, bu.bobject_university_id, bu.year_start,
            bu.year_end, pd.name degree, pm.name majors
            from `project_id.dataset_name.table_name` bu
            left join `project_id.dataset_name.table_name` pd on bu.degree_id = pd.id
            left join `project_id.dataset_name.table_name` bum on bu.id = bum.bobjects_universities_id
            left join `project_id.dataset_name.table_name` pm on bum.major_id = pm.id
            where bobject_user_id = @from_id
            and bobject_university_id not in (select bobject_university_id
            from `project_id.dataset_name.table_name` bu
            where bobject_user_id = @to_id)"""

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("from_id", "INT64", from_id),
                bigquery.ScalarQueryParameter("to_id", "INT64", to_id)
            ]
        )

        query_job = self.client.query(from_people_university_data, job_config)
        results = query_job.result().total_rows
        universities = {}
        if results > 0:
            majors = []
            university_id = []
            for i, row in enumerate(query_job):

                if i == 0:
                    majors.append(row["majors"])
                    university_id.append(row["bobject_university_id"])

                if row["bobject_university_id"] not in university_id and i > 0:
                    majors = []
                    university_id.append(row["bobject_university_id"])

                if row["bobject_university_id"] in university_id and i > 0:
                    majors.append(row["majors"])

                education = {
                    "university_id": row["bobject_university_id"],
                    "degree": row["degree"],
                    "majors": utils.unique_list(majors),
                    "year_start": row["year_start"],
                    "year_end": row["year_end"]
                }

                education = {k: v for k, v in education.items() if v not in [None, "", []]}

                universities[row["bobject_university_id"]] = education

        return universities

    def get_investments_data(
        self, from_id: int, to_id: int
    ) -> Dict[int, Dict[str, Union[int, bool]]]:

        """Get all the investments which need to be trandferred

        Args:
            from_id (str): duplicate company from which data points need to be transfered
            to_id(str): correct company to which data points need to be transfered

        Returns:
            dict: contains investment data need to be transferred
        """

        non_existing: str = """select distinct(ci.company_bobject_id), ci.investor_bobject_id, CASE WHEN ci.is_exited = 0 then False WHEN ci.is_exited = 1 then True END is_exited
            from `project_id.dataset_name.table_name` ci
            left join `project_id.dataset_name.table_name` bf on bf.bobject_id = ci.company_bobject_id 
            left join `project_id.dataset_name.table_name` bo on bo.id = ci.company_bobject_id
            left join `project_id.dataset_name.table_name` bo2 on bo2.id = ci.investor_bobject_id 
            where ci.is_exited=0 and bo2.id in (@from_id) and ci.company_bobject_id not in (select distinct(ci.company_bobject_id) 
            from `project_id.dataset_name.table_name` ci
            left join `project_id.dataset_name.table_name` bf on bf.bobject_id = ci.company_bobject_id 
            left join `project_id.dataset_name.table_name` bo on bo.id = ci.company_bobject_id
            left join `project_id.dataset_name.table_name` bo2 on bo2.id = ci.investor_bobject_id
            where ci.is_exited=0 and bo2.id IN (@to_id))"""

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "from_id", "INT64", from_id
                ),
                bigquery.ScalarQueryParameter(
                    "to_id", "INT64", to_id
                ),
            ]
        )

        query_job = self.client.query(non_existing, job_config)
        KEY = "company_bobject_id"
        return {
            row[KEY]: {
                "{company}_id": to_id,
                 "payload":{
                    "company_id": row[KEY],
                    "is_exited": row['is_exited'],
                 }
            }
            for row in query_job
        }

    def get_team_data(
        self, from_id: int, to_id: int, entity_type: str
    ) -> dict:

        """Get all the team data which need to be trandferred

        Args:
            from_id (str): duplicate company from which data points need to be transfered
            to_id(str): correct company to which data points need to be transfered

        Returns:
            dict: contains team data need to be transferred
        """

        job_id = utils.job_id(entity_type=entity_type, data_type="teams")
        base = {"overwrite": False, "job_type": "update_user", "job_id": job_id}

        non_existing: str = """select distinct(ci.bobject_user_id), CASE WHEN ci.is_past = 0 then False WHEN ci.is_past = 1 then True END is_past,
            pt.name title, bo2.type, bo2.url, ci.month_start, ci.month_end, ci.year_start, ci.year_end
            from `project_id.dataset_name.table_name` ci inner join `project_id.dataset_name.table_name` bo on ci.bobject_company_id = bo.id
            inner join `project_id.dataset_name.table_name` bo2 on ci.bobject_user_id = bo2.id
            left join `project_id.dataset_name.table_name` bfl on ci.bobject_user_id = bfl.bobject_id
            left join `project_id.dataset_name.table_name` cut on ci.id = cut.company_user_id
            left join `project_id.dataset_name.table_name` pt on cut.title_id = pt.id
            where ci.bobject_company_id in (@from_id)
            and bo.type IN (2,3,4,6,12,15,16,17)
            and (bo2.is_editorial = 0 or bfl.flag_id in (1,2,7,5946) or ifnull(bfl.flag_id, 0) = 0)
            and ci.bobject_user_id not in
            (select distinct(ci.bobject_user_id)
            from `project_id.dataset_name.table_name` ci inner join `project_id.dataset_name.table_name` bo on ci.bobject_company_id = bo.id
            inner join `project_id.dataset_name.table_name` bo2 on ci.bobject_user_id = bo2.id
            left join `project_id.dataset_name.table_name` bfl on ci.bobject_user_id = bfl.bobject_id
            left join `project_id.dataset_name.table_name` cut on ci.id = cut.company_user_id
            left join `project_id.dataset_name.table_name` pt on cut.title_id = pt.id
            where ci.bobject_company_id in (@to_id)
            and bo.type IN (2,3,4,6,12,15,16,17)
            and (bo2.is_editorial = 0 or bfl.flag_id in (1,2,7,5946) or ifnull(bfl.flag_id, 0) = 0))"""

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "from_id", "INT64", from_id
                ),
                bigquery.ScalarQueryParameter(
                    "to_id", "INT64", to_id
                ),
            ]
        )

        query_job = self.client.query(non_existing, job_config)
        team = {}
        titles = []
        user_id = []
        for i, row in enumerate(query_job):

            if i == 0:
                titles.append(row["title"])
                user_id.append(row["bobject_user_id"])

            if row["bobject_user_id"] not in user_id and i > 0:
                titles = []
                user_id.append(row["bobject_user_id"])

            if row["bobject_user_id"] in user_id and i > 0:
                titles.append(row["title"])

            company = {
                "company_id": to_id,
                "month_start": row["month_start"],
                "year_start": row["year_start"],
                "month_end": row["month_end"],
                "year_end": row["year_end"],
                "is_past": row["is_past"],
                "titles": utils.unique_list(titles),
            }

            company = {k: v for k, v in company.items() if v not in [None, ""]}

            team[row["bobject_user_id"]] = {
                **base,
                "{company}_id": row["bobject_user_id"],
                "payload": {"companies": [company]},
            }

        return team
