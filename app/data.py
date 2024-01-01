import pandas as pd
from sodapy import Socrata

import requests
from datetime import datetime, timedelta
from cachetools import cached, TTLCache

# Unauthenticated client only works with public data sets. Note 'None'
# in place of application token, and no username or password:
client = Socrata("data.cityofnewyork.us", None)

cyclists_injured = "number_of_cyclist_injured>0 OR number_of_cyclist_killed>0"
pedestrians_injured = (
    "number_of_pedestrians_injured>0 OR number_of_pedestrians_killed>0"
)

# only return crashes where at least one cyclist or pedestrian was injured or killed
all_records_with_injury = cyclists_injured + " OR " + pedestrians_injured

cache = TTLCache(maxsize=1, ttl=86400)


# call API and cache results for 24 hours
@cached(cache)
def get_api_data():
    print("getting updated data")

    results = client.get("h9gi-nx95", limit=500000, where=all_records_with_injury)
    results_df = pd.DataFrame.from_records(results)
    print("results updated")
    return results_df


def get_data():
    results_df = get_api_data()
    print("get data called")
    return results_df


# pandas DataFrame
results_df = get_data()

# cyclists and pedestrians dataframes
cyclist_df = results_df[
    (results_df["number_of_cyclist_injured"].astype(int) > 0)
    | (results_df["number_of_cyclist_killed"].astype(int) > 0)
].copy()

pedestrian_df = results_df[
    (results_df["number_of_pedestrians_injured"].astype(int) > 0)
    | (results_df["number_of_pedestrians_killed"].astype(int) > 0)
].copy()

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)

# convert crash_dat to datetime
cyclist_df["crash_date"] = pd.to_datetime(cyclist_df["crash_date"])
cyclist_df["crash_time"] = pd.to_datetime(cyclist_df["crash_time"])
pedestrian_df["crash_date"] = pd.to_datetime(pedestrian_df["crash_date"])
pedestrian_df["crash_time"] = pd.to_datetime(pedestrian_df["crash_time"])

# calculate yearly totals
yearly_cyclist_totals = (
    cyclist_df.groupby(cyclist_df["crash_date"].dt.year)["crash_date"]
    .agg("count")
    .to_dict()
)
yearly_pedestrian_totals = (
    pedestrian_df.groupby(pedestrian_df["crash_date"].dt.year)["crash_date"]
    .agg("count")
    .to_dict()
)

sum_cyclist_injuries = results_df["number_of_cyclist_injured"].astype(int).sum()
sum_cyclist_deaths = results_df["number_of_cyclist_killed"].astype(int).sum()
sum_pedestrian_injuries = results_df["number_of_pedestrians_injured"].astype(int).sum()
sum_pedestrian_deaths = results_df["number_of_pedestrians_killed"].astype(int).sum()

# current date
today = datetime.today()
date_cutoff = (today - timedelta(days=365)).strftime("%Y-%m-%d")

this_year_df = results_df[
    (results_df["crash_date"] >= "2024-01-01")
    & (results_df["crash_date"] < today.strftime("%Y-%m-%d"))
]
last_year_df = results_df[
    (results_df["crash_date"] >= "2023-01-01")
    & (results_df["crash_date"] < date_cutoff)
]

sum_this_year_cyclist_injuries = (
    this_year_df["number_of_cyclist_injured"].astype(int).sum()
)
sum_this_year_cyclist_deaths = (
    this_year_df["number_of_cyclist_killed"].astype(int).sum()
)
sum_this_year_pedestrian_injuries = (
    this_year_df["number_of_pedestrians_injured"].astype(int).sum()
)
sum_this_year_pedestrain_deaths = (
    this_year_df["number_of_pedestrians_killed"].astype(int).sum()
)
sum_last_year_cyclist_injuries = (
    last_year_df["number_of_cyclist_injured"].astype(int).sum()
)
sum_last_year_cyclist_deaths = (
    last_year_df["number_of_cyclist_killed"].astype(int).sum()
)
sum_last_year_pedestrian_injuries = (
    last_year_df["number_of_pedestrians_injured"].astype(int).sum()
)
sum_last_year_pedestrian_deaths = (
    last_year_df["number_of_pedestrians_killed"].astype(int).sum()
)

# calculate day of week totals
cyclist_day_counts = cyclist_df["crash_date"].dt.day_name().value_counts().to_dict()
pedestrian_day_counts = (
    pedestrian_df["crash_date"].dt.day_name().value_counts().to_dict()
)

# calculate hour totals
cyclist_df["hour"] = cyclist_df["crash_time"].dt.hour
cyclist_hour_counts = cyclist_df["hour"].value_counts().to_dict()

hourly_cyclist_counts = []
for key in sorted(cyclist_hour_counts.keys()):
    hourly_cyclist_counts.append({"x": key, "y": cyclist_hour_counts[key]})

pedestrian_df["hour"] = pedestrian_df["crash_time"].dt.hour
pedestrian_hour_counts = pedestrian_df["hour"].value_counts().to_dict()

hourly_pedestrian_counts = []
for key in sorted(pedestrian_hour_counts.keys()):
    hourly_pedestrian_counts.append({"x": key, "y": pedestrian_hour_counts[key]})


# this function finds how many times a given month has occurred since 2012,
# and returns the denominator to be used in monthly average calculation
def find_denominator_month(month):
    total_months = datetime.today().year - 2013
    if month >= 7:  # account for the limited data in 2012(jan-july)
        total_months += 1
    if datetime.today().month >= month:  # if the month has already occurred this year
        total_months += 1
    return total_months


# this function calculates the monthly average for a given count
def calculate_monthly_average(count):
    averages = []
    for i in range(1, 13):
        averages.append(round(count[i] / find_denominator_month(i)))
    return averages


months = range(1, 13)  # All 12 months
cyclist_monthly_totals = {month: 0 for month in months}
pedestrian_monthly_totals = {month: 0 for month in months}

for value in cyclist_df["crash_date"]:
    cyclist_monthly_totals[value.month] += 1
for value in pedestrian_df["crash_date"]:
    pedestrian_monthly_totals[value.month] += 1

# calculate monthly averages
cyclist_monthly_average = calculate_monthly_average(cyclist_monthly_totals)
pedestrian_monthly_average = calculate_monthly_average(pedestrian_monthly_totals)

# object to be returned
data_object = {
    "counterData": {
        "ytdCyclistInjuries": str(sum_this_year_cyclist_injuries),
        "ytdCyclistDeaths": str(sum_this_year_cyclist_deaths),
        "ytdPedestrianInjuries": str(sum_this_year_pedestrian_injuries),
        "ytdPedestrianDeaths": str(sum_this_year_pedestrain_deaths),
        "lastYtdCyclistInjuries": str(sum_last_year_cyclist_injuries),
        "lastYtdCyclistDeaths": str(sum_last_year_cyclist_deaths),
        "lastYtdPedestrianInjuries": str(sum_last_year_pedestrian_injuries),
        "lastYtdPedestrianDeaths": str(sum_last_year_pedestrian_deaths),
    },
    "yearlyData": {
        "yearlyCyclistTotals": yearly_cyclist_totals,
        "yearlyPedestrianTotals": yearly_pedestrian_totals,
    },
    "weekDayData": {
        "cyclistDayCounts": cyclist_day_counts,
        "pedestrianDayCounts": pedestrian_day_counts,
    },
    "hourlyData": {
        "hourlyCyclistTotals": hourly_cyclist_counts,
        "hourlyPedestrianTotals": hourly_pedestrian_counts,
    },
    "monthlyData": {
        "monthlyCyclistAverages": cyclist_monthly_average,
        "monthlyPedestrianAverages": pedestrian_monthly_average,
    },
}
