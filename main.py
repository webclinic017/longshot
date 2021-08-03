import pandas as pd
from database.market import Market
from database.sec import SEC
from modeler.modeler import Modeler as m
from preprocessor.model_preprocessor import ModelPreprocessor as mp
from strategy.quarterly_financial_categorical import QuarterlyFinancialCategorical
from datetime import datetime, timedelta
import numpy as np
import math
from tqdm import tqdm

market = Market()
sec = SEC("sec")
year_range = range(2017,2018)
quarter_range = range(1,2)
qfc = QuarterlyFinancialCategorical()

market.connect()
sp5 = market.retrieve_data("sp500")
prices = market.retrieve_data("prices")
quarterly_classification_data = market.retrieve_data("dataset_quarter_classification")
quarterly_regression_data = market.retrieve_data("dataset_quarter_regression")
market.close()
quarterly_classification_data = quarterly_classification_data[:-1]
quarterly_regression_data = quarterly_regression_data[:-1]

prices["year"]  = [x.year for x in prices["date"]]
prices["quarter"]  = [x.quarter for x in prices["date"]]

quarterly_grouped = prices.groupby(["year","quarter","ticker"]).max()
quarterly_grouped["category"] = [math.ceil(x / 100 ) * 100 for x in quarterly_grouped["adjclose"]]
quarterly_grouped["category"] = [250 if x > 100 else x for x in quarterly_grouped["category"]]
quarterly_grouped["category"] = [500 if x > 250 else x for x in quarterly_grouped["category"]]
quarterly_grouped["category"] = [1000 if x > 500 else x for x in quarterly_grouped["category"]]
quarterly_grouped["category"] = [2000 if x > 1000 else x for x in quarterly_grouped["category"]]
quarterly_grouped["category"] = [3000 if x > 2000 else x for x in quarterly_grouped["category"]]

quarterly_grouped.reset_index(inplace=True)
groups = quarterly_grouped.merge(sp5.rename(columns={"Symbol":"ticker"}), on = "ticker",how="left")

yearly_gap = 1
training_years = 4
sec.connect()
qfc.db.connect()
qfc.db.drop_table("sim")
qfc.db.drop_table("models")
qfc.db.close()
fails = []
for year in tqdm(year_range):
    for quarter in tqdm(quarter_range):
        for sector in sp5["GICS Sector"].unique():
            sector_groups = groups[(groups["GICS Sector"] == sector) & (groups["year"] == year) & (groups["quarter"] == quarter)]
            for category in sector_groups["category"].unique():
                try:
                    category_groups = sector_groups[sector_groups["category"] == category]
                    training_datas = []
                    prediction_datas = []
                    relevant_columns = []
                    tickers = []
                    for cik in list(category_groups["CIK"].unique()):
                        try:
                            filing = sec.retrieve_filing_data(int(cik))
                            symbols = sp5[sp5["CIK"]==cik]["Symbol"]
                            if symbols.index.size > 1:
                                ticker = str(list(symbols)[0])
                            else:
                                ticker = symbols.item()
                            classification = quarterly_classification_data[["year","quarter",ticker]].copy()
                            regression = quarterly_regression_data[["year","quarter",ticker]].copy()
                            datasets = {"funds":filing.copy(),"regression":regression,"classification":classification}
                            training_data, prediction_data = qfc.transform(datasets,ticker,year,quarter,yearly_gap,training_years)
                            relevant_columns.append(list(training_data.columns))
                            training_datas.append(training_data)
                            prediction_datas.append(prediction_data)
                            tickers.append(ticker)
                        except Exception as e:
                            print("prep",year,quarter,sector,category,ticker,str(e))
                            fails.append([year,quarter,sector,category,ticker,str(e)])
                    try:
                        if len(tickers) > 0:
                            training_columns = relevant_columns[0]
                            for rc in relevant_columns[1:]:
                                training_columns = list(set(training_columns) & set(rc))
                            if len(training_columns) > 0 and training_data.index.size > 0:
                                td = pd.concat(training_datas)[training_columns]
                                predd = pd.DataFrame(prediction_datas)[training_columns]
                                refined_data = mp.preprocess(td.copy())
                                factors = list(refined_data["X"].columns)
                                regression_models = m.regression(refined_data.copy(),ranked=False,tf=False,deep=False)
                                classification_models = m.classification(refined_data.copy(),tf=False,deep=False,multioutput=False)
                                models = pd.DataFrame([regression_models,classification_models])
                                for i in range(len(tickers)):
                                    try:
                                        ticker = tickers[i]
                                        prediction_data = predd.iloc[i][factors]
                                        sim = qfc.create_sim(ticker,year,yearly_gap,quarter,models,factors,{"funds":pd.DataFrame([prediction_data])})
                                    except Exception as e:
                                        print("sim",year,quarter,sector,category,ticker,str(e))
                                qfc.store_models(year,quarter,sector,category,models)
                        else:
                            print(year,quarter,sector,category,"not enough data for group")
                            continue
                    except Exception as e:
                        print("mid",str(e))

                except Exception as e:
                    print(year,quarter,sector,category,str(e))
sec.close()