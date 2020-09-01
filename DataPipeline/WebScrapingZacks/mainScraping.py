import pandas as pd
import os

from ZacksWebScraping import tabScrap

if __name__ == "__main__":
    # folders paths
    ROOT_DIR = '/Users/davideconcu/Documents/Stock Analysis/'
    epsHistoricalPath = ROOT_DIR + 'DataPipeline/WebScrapingZacks/epsHistorical'
    fundHistoricalPath = ROOT_DIR + 'DataPipeline/WebScrapingZacks/FundamentalsHistorical'
    symbPath = ROOT_DIR + 'DataPipeline/WebScrapingZacks/docs/Symbols.csv'
    fundPath = ROOT_DIR + 'DataPipeline/WebScrapingZacks/docs/FundamentalsList.csv'
    failQueriesFundamentalFile = ROOT_DIR + 'DataPipeline/WebScrapingZacks/docs/failed_queries_Fundamentals.txt'
    
    # create folders
    if not os.path.exists(epsHistoricalPath):
        os.mkdir(epsHistoricalPath)
    if not os.path.exists(fundHistoricalPath):
        os.mkdir(fundHistoricalPath)

    tickersData = pd.read_csv(symbPath)
    tickers = list(tickersData['Symbol'])
    zScraping = tabScrap()

    # scraping selector
    performEpsScraping = False
    performFundamentalsScraping = True
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


