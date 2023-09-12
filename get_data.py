# bigquery
import os
import hashlib
import json
import pandas as pd
from datetime import datetime, timedelta
from modules.settings import client, template_env, PROJECT

# retrieve data
def template_retrieval(sql_file, **kwargs):
    sql = template_env.get_template(sql_file).render(**kwargs)
    output_df = client.query(sql, location="asia-east1").to_dataframe()
    if len(output_df) == 0:
        os.makedirs("errors", exist_ok=True)
        output_name = hashlib.sha256(sql.encode("utf-8")).hexdigest()
        with open("errors/" + output_name + ".sql", "w") as f:
            f.write(sql)
    return output_df


def retrieve_kol_post_time(
    platform: str, start_week: int, end_week: int, zh_country_name: str
):
    """
    Retrieve KOL post time
    """
    assert end_week > start_week
    start_week = start_week - 6 # need to minus 6 for past post time 

    return template_retrieval(
        f"post_time_{platform}.sql",
        platform=platform,
        start_week=start_week,
        end_week=end_week,
        zh_country_name=zh_country_name,
        project=PROJECT,
    )

def retrieve_property2(
    platform: str, post_id2kol_id: dict, start_week: int, end_week: int
):
    """
    convert autotagv2 to property match design
        post_property  times  kol_id  week_num platform
    0            遊戲      1       5    202228       fb
    1            空標      1       5    202230       fb
    2            空標      2       6    202226       fb
    """
    assert end_week > start_week 
    df = template_retrieval(        
        "post_renata_autotag.sql",
        platform=platform,
        start_week=start_week,
        end_week=end_week,
        project=PROJECT,
    )
    data_agg = {}
    for _, row in df.iterrows():
        if row["post_id"] not in post_id2kol_id:
            continue

        key = "{}_{}_{}".format(
            post_id2kol_id[row["post_id"]], row["week_num"], row["platform"]
        )
        if key not in data_agg:
            data_agg[key] = {}
        if row["model_output"] is None:
            tags = []
        else:
            tags = json.loads(row["model_output"])
        if tags is None:
            tags = []

        for t in tags:
            if t["tag_name"] not in data_agg[key]:
                data_agg[key][t["tag_name"]] = 0
            data_agg[key][t["tag_name"]] += 1
    output_data = []
    for key, counts in data_agg.items():
        kol_id, week_num, platform = key.split("_")
        kol_id = int(kol_id)
        week_num = int(week_num)
        for post_property, times in counts.items():
            output_data.append(
                {
                    "times": times,
                    "post_property": post_property,
                    "kol_id": kol_id,
                    "week_num": week_num,
                    "platform": platform,
                }
            )

    return pd.DataFrame(output_data)


def retrieve_post(platform: str, start_week: int, end_week: int, zh_country_name: str):
    assert end_week > start_week
    start_week = start_week - 6  # need to minus 6 for past time 
    sql_file = "retrieve_post_{}.sql".format(platform)
    # we need different sql file cause the equation for weighted score
    # is different
    return template_retrieval(
        sql_file,
        zh_country_name=zh_country_name,
        platform=platform,
        start_week=start_week,
        end_week=end_week,
        project=PROJECT,
    )

# for follower count
def retrieve_follower_count(
    platform: str, start_week: int, end_week: int, inference: bool
):
    assert end_week > start_week
    # we neeed to add -1 to ensure it knows to start at monday
    start_week_date = datetime.strptime(str(start_week) + "-1", "%Y%W-%w")
    print("start week",start_week_date)
    end_week_date = datetime.strptime(str(end_week) + "-1", "%Y%W-%w")
    print("end week",end_week_date)
    # we only look back 6 but for safety we add 1, why?
    # look_back = 6 + 1
    look_back = 6
    data_start_week = start_week_date - timedelta(days=7 * look_back)
    print("historical start week ", data_start_week)

    if inference:
        data_end_week = end_week_date
    else:
        # we predict 8 weeks future, but for safety add 1, why?
        # look_future = 8 + 1
        look_future = 8 
        data_end_week = end_week_date + timedelta(days=look_future * 7)
        print("predict_future_week ", data_end_week)

    data_start_week_str = data_start_week.strftime("%Y%W")
    data_end_week_str = data_end_week.strftime("%Y%W")

    return template_retrieval(

        "follower_count.sql" if not inference else "inference_follower_count.sql",
        platform=platform,
        historical_start_week=int(data_start_week_str),
        predict_future_week=int(data_end_week_str),
        start_week=int(start_week),
        end_week=int(end_week),
        project=PROJECT,
    )

# Not used
def retrieve_property(platform: str, start_week: int, end_week: int):
    return template_retrieval(
        "post_property.sql", platform=platform, start_week=start_week, end_week=end_week
    )

# Not used
def retrieve_content(platform: str, start_week: int, end_week: int):
    sql_file = "post_content.sql"
    return template_retrieval(
        sql_file, platform=platform, start_week=start_week, end_week=end_week
    )