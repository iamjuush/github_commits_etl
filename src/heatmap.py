import pandas as pd

ORGANISATION = "apache"
REPOSITORY = "hadoop"

INTERVAL_MAPPING = {
    0: "12am-3am",
    1: "3am-6am",
    2: "6am-9am",
    3: "9am-12pm",
    4: "12pm-3pm",
    5: "3pm-6pm",
    6: "6pm-9pm",
    7: "9pm-12am"
}

DAY_OF_WEEK_MAPPING = {
    1: "Mon",
    2: "Tue",
    3: "Wed",
    4: "Thurs",
    5: "Fri",
    6: "Sat",
    7: "Sun"
}


def df_to_heatmap(df: pd.DataFrame) -> pd.DataFrame:
    # Do some renaming to map to human readable terms
    df["interval_of_day"] = df["interval_of_day"].apply(lambda x: INTERVAL_MAPPING[x])
    df["day_of_week"] = df["day_of_week"].apply(lambda x: DAY_OF_WEEK_MAPPING[x])

    # Pivot table to get heatmap
    heatmap = pd.pivot_table(df, values="commit_counts", index=["interval_of_day"], columns=["day_of_week"])

    # Rearrange column ordering
    heatmap = heatmap[["Mon", "Tue", "Wed", "Thurs", "Fri", "Sat", "Sun"]]
    return heatmap