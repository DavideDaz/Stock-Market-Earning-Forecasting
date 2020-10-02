import pandas as pd
import os
import sys
import mergeTables

if __name__ == "__main__":


    if not os.path.exists('DataPipeline/DataPreprocessing/MergedTables'):
        os.mkdir('DataPipeline/DataPreprocessing/MergedTables')

    tickersData = pd.read_csv('DataPipeline/WebScrapingZacks/docs/Symbols.csv')
    fundamentals = pd.read_csv('DataPipeline/WebScrapingZacks/docs/FundamentalsList.csv')
    mergeT = mergeTables.tabMerge()
    mergeT.mergeTab(tickersData,fundamentals)
