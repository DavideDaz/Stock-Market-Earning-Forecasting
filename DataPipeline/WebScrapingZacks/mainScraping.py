import pandas as pd
import os


import ZacksWebScraping

if __name__ == "__main__":
    # folders paths
    wd = os.getcwd()
    epsHistoricalPath = '/epsHistorical'
    fundHistoricalPath = '/FundamentalsHistorical'
    symbPath = '/docs/Symbols.csv'
    fundPath = '/docs/FundamentalsList.csv'
    
    # create folders
    if not os.path.exists(wd + epsHistoricalPath):
        os.mkdir(wd + epsHistoricalPath)
    if not os.path.exists(wd + fundHistoricalPath):
        os.mkdir(wd + fundHistoricalPath)

    tickersData = pd.read_csv(wd + symbPath)
    tickers = list(tickersData['Symbol'])
    zScraping = ZacksWebScraping.tabScrap()

    # scraping selector
    performEpsScraping = False
    performFundamentalsScraping = True
    errorFix = False

    if performEpsScraping:
        zScraping.epsScraping(tickers,wd)
    
    if performFundamentalsScraping and not errorFix:
        print('#### Performing Web Scraping ####')
        fundamentalsData = pd.read_csv(wd + fundPath)
        fundamentalsListMark,fundamentalsListFreq = list(fundamentalsData['Mark']),list(fundamentalsData['TimeFrequency'])
        fundamentalsList = [(x,y) for x,y in zip(fundamentalsListMark,fundamentalsListFreq)]
        zScraping.fundamentalsScraping(tickers,fundamentalsList,wd,errorFix)

    if errorFix:
        fundamentalsData = pd.read_csv(wd + fundPath)
        f = wd+'/docs/failed_queries_Fundamentals.txt'
        zScraping.fixErrorTickers(f,fundamentalsData,wd)

    print('#### End of Web Scraping ####')


