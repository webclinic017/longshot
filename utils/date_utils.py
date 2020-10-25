from pandas import date_range, DataFrame
from datetime import datetime, timedelta

class DateUtils(object):
    @classmethod
    def create_timeline(self,start,end):
        rows = []
        base = date_range(start,end)
        for date in base:
            quarter = date.quarter
            year = date.year
            week = date.week
            rows.append({"date":date,"year":year,"quarter":quarter,"week":week})
        timeline = DataFrame(rows)
        return timeline
    
    @classmethod
    def create_daily_training_range(self,date,training_days,prediction_dates):
        ts = str(date - timedelta(days=(training_days + prediction_dates))).split(" ")[0]
        te = str(date - timedelta(days=(prediction_dates + 1))).split(" ")[0]
        prd = str(date).split(" ")[0]
        return [datetime.strptime(x,"%Y-%m-%d") if datetime.strptime(x,"%Y-%m-%d").weekday() < 5 else datetime.strptime(x,"%Y-%m-%d") + timedelta(days=7-datetime.strptime(x,"%Y-%m-%d").weekday()) for x in [ts,te,prd]]

        
    @classmethod
    def create_quarterly_training_range(self,timeline,year,quarter,training_years,gap):
        current_quarter = timeline[(timeline["quarter"] == quarter) & (timeline["year"] == year)]
        previous_year = timeline[(timeline["quarter"] == quarter) & (timeline["year"] == year-training_years)]
        dates = timeline[(timeline["quarter"] == quarter) & (timeline["year"] == year)]
        ts = previous_year.iloc[0]["date"]
        te = current_quarter.iloc[0]["date"] - timedelta(days=1)
        ps = current_quarter.iloc[0]["date"] + timedelta(days=gap)
        pe = current_quarter.iloc[len(current_quarter)-1]["date"] + timedelta(days=gap)
        return [x.strftime("%Y-%m-%d") if x.weekday() < 5 else (x + timedelta(days=7-x.weekday())).strftime("%Y-%m-%d") for x in [ts,te,ps,pe]]
    
    @classmethod
    def create_weekly_training_range(self,timeline,year,week,training_years):
        current_week = timeline[(timeline["week"] == week) & (timeline["year"] == year)]
        previous_year = timeline[(timeline["week"] == week) & (timeline["year"] == year-training_years)]
        dates = timeline[(timeline["week"] == week) & (timeline["year"] == year)]
        ts = previous_year.iloc[0]["date"]
        te = current_week.iloc[0]["date"] - timedelta(days=1)
        ps = current_week.iloc[0]["date"]
        pe = current_week.iloc[len(current_week)-1]["date"]
        return [x.strftime("%Y-%m-%d") if x.weekday() < 5 else (x + timedelta(days=7-x.weekday())).strftime("%Y-%m-%d") for x in [ts,te,ps,pe]]