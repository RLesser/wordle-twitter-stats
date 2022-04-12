import os
import pandas_gbq


def get_wordle_rounds_count_data():
    project_id = os.environ.get("GCP_PROJECT")
    query = f"""
      SELECT * FROM `{project_id}.main.wordle_rounds_count`
    """

    df = pandas_gbq.read_gbq(query, project_id=project_id)

    df.to_csv("data_views/wordle_rounds_count.csv", index=False)


if __name__ == "__main__":
    get_wordle_rounds_count_data()
