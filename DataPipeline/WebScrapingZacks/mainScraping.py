import pandas as pd
import os

from ZacksWebScraping import tabScrap

if __name__ == "__main__":
    # folders paths
    epsHistoricalPath = 'DataPipeline/WebScrapingZacks/epsHistorical'
    fundHistoricalPath = 'DataPipeline/WebScrapingZacks/FundamentalsHistorical'
    symbPath = 'DataPipeline/WebScrapingZacks/docs/Symbols.csv'
    fundPath = 'DataPipeline/WebScrapingZacks/docs/FundamentalsList.csv'
    failQueriesFundamentalFile = 'DataPipeline/WebScrapingZacks/docs/failed_queries_Fundamentals.txt'
    
    # create folders
    if not os.path.exists(epsHistoricalPath):
        os.mkdir(epsHistoricalPath)
    if not os.path.exists(fundHistoricalPath):
        os.mkdir(fundHistoricalPath)

    tickersData = pd.read_csv(symbPath)
    tickers = list(tickersData['Symbol'])
    zScraping = tabScrap()

    # scraping selector
    performEpsScraping = True
    performFundamentalsScraping = False
    errorFix = False

    if performEpsScraping:
        zScraping.epsScraping(tickers)
    
    if performFundamentalsScraping and not errorFix:
        print('#### Performing Web Scraping ####')
        fundamentalsData = pd.read_csv(fundPath)
        fundamentalsListMark,fundamentalsListFreq = list(fundamentalsData['Mark']),list(fundamentalsData['TimeFrequency'])
        fundamentalsList = [(x,y) for x,y in zip(fundamentalsListMark,fundamentalsListFreq)]
        zScraping.fundamentalsScraping(tickers,fundamentalsList,errorFix)

    if errorFix:
        fundamentalsData = pd.read_csv(fundPath)
        zScraping.fixErrorTickers(failQueriesFundamentalFile,fundamentalsData)

    print('#### End of Web Scraping ####')


