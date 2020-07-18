from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import selenium
import pandas as pd
import os
import sys


import ZacksWebScraping

if __name__ == "__main__":
    wd = os.getcwd()
    tickersData = pd.read_csv(wd+'/docs/Symbols.csv')
    tickers = list(tickersData['Symbol'])
    sector = list(tickersData['GICS Sector'])

    tickers = tickers[2:3] + tickers[4:20]

    tickerErrorEPS = []
    tickerErrorFundamentals = []

    zScraping = ZacksWebScraping.tabScrap()

    performEpsScraping = False
    performFundamentalsScraping = True

    if performEpsScraping:
        for t in tickers:
            try:
                opts = Options()
                opts.headless = True
                driver = webdriver.Firefox(options=opts)
                driver.get("https://www.zacks.com/stock/chart/{}/price-consensus-eps-surprise-chart".format(t))

                title  = 'chart_wrapper_datatable_eps_surprise'
                
                data = zScraping.getTable(title,driver,t,'EPS Surprise','%')

                data.to_csv(wd + '/epsHistorical/{}_eps_surprise.csv'.format(t), na_rep='NaN')

                driver.close()

            except selenium.common.exceptions.NoSuchElementException:
                tickerErrorEPS.append(t)
                print('Ticker Error {}: EPS Table not available'.format(t))
                driver.close()
            except Exception as e:
                tickerErrorEPS.append(t)
                print('Error during {tk} EPS Scraping! Code: {c}, Message, {m}'.format(tk = t, c = type(e).__name__, m = str(e)))
                tickerErrorEPS.append(str(e))
                driver.close()

        #Save failed queries to a text file to retry
        if len(tickerErrorEPS) > 0:
            with open(wd+'/docs/failed_queries_EPS.txt','w') as outfile:
                for name in tickerErrorEPS:
                    outfile.write(name+'\n')
    
    if performFundamentalsScraping:
        fundamentalsData = pd.read_csv(wd+'/docs/FundamentalsList.csv')
        fundamentalsListMark = list(fundamentalsData['Mark'])
        fundamentalsListUnit = list(fundamentalsData['Unit'])

        fundamentalsList = [(x,y) for x,y in zip(fundamentalsListMark,fundamentalsListUnit)]

        for t in tickers:
                for f,u in fundamentalsList:
                    try:
                        opts = Options()
                        opts.headless = True
                        driver = webdriver.Firefox(options=opts)
                        driver.get("https://www.zacks.com/stock/chart/{}/fundamental/{}".format(t,f))
                        
                        data = zScraping.getTable(None,driver,t,f,u)

                        if not os.path.isdir(wd + '/FundamentalsHistorical/' + t):
                            os.mkdir(wd + '/FundamentalsHistorical/' + t)
                            
                        data.to_csv(wd + '/FundamentalsHistorical/' + t + '/{}_{}.csv'.format(t,f), na_rep='NaN')
                        driver.close()

                    except selenium.common.exceptions.NoSuchElementException:
                        tickerErrorFundamentals.append([t,f])
                        print('Ticker Error {}: Fundamentals Table {} not available'.format(t,f))
                        driver.close()
                    except Exception as e:
                        tickerErrorFundamentals.append([t,f])
                        print('Error during {tk} Fundamental Scraping of {ft}! Code: {c}, Message, {m}'.format(tk = t,ft = f, c = type(e).__name__, m = str(e)))
                        driver.close()

        #Save failed queries to a text file to retry
        if len(tickerErrorFundamentals) > 0:
            with open(wd+'/docs/failed_queries_Fundamentals.txt','w') as outfile:
                for name,mark in tickerErrorFundamentals:
                    outfile.write(name + ' ' + mark + '\n')
   


