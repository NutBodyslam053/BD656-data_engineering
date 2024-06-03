import pandas as pd
from datetime import datetime


def convert_to_datetime(timestamp):
    return datetime.fromtimestamp(timestamp)


def reddit_dataengineering_clean(
    json_data: dict,
    # schema_fields: dict,
    ingest_datetime: datetime,
) -> pd.DataFrame:

    data = [i["data"] for i in json_data["data"]["children"]]
    df = pd.DataFrame(data)

    cols = [
        "created",
        "id",
        "author",
        "title",
        "selftext",
        "num_comments",
        "score",
        "ups",
        "downs",
        "upvote_ratio",
        "link_flair_text",
    ]
    df = df[cols].copy()

    df["created"] = df["created"].map(convert_to_datetime)

    today_date = datetime.now().date()
    df = df[df["created"].dt.date == today_date].copy()

    df.rename(
        columns={
            "created": "submission_date",
            "id": "submission_id",
            "author": "author_name",
            "title": "submission_title",
            "selftext": "submission_text",
            "num_comments": "num_comments",
            "score": "score",
            "ups": "ups",
            "downs": "downs",
            "upvote_ratio": "upvote_ratio",
            "link_flair_text": "category",
        },
        inplace=True,
    )

    # Generate column "ingest_date" and "ingest_datetime"
    df["ingest_date"] = ingest_datetime.date()
    # df["ingest_datetime"] = ingest_datetime.replace(microsecond=0)
    # df["ingest_datetime"] = df["ingest_datetime"].dt.tz_localize(None)
    # df["ingest_datetime"] = df["ingest_datetime"].astype("datetime64[ms]")

    # # Rearange columns
    # schema_col_names = pd.DataFrame(schema_fields)["name"].to_list()
    # df = df[df.columns.intersection(schema_col_names)].copy()

    return df
