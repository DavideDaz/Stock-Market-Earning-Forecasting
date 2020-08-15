import os
import pandas as pd 
import torch
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler

class dataSequenceGenerator():
    def __init__(self, sequenceLength, targetLength, targetFeatures:list,splitRatio,dataScaling:str):
        self.dfFolderPath = '../WebScraping/MergedTables'
        self.symbolsTable = '../WebScraping/docs/Symbols.csv'
        self.discardedCompaniesList = '/docs/discardedList.txt'
        self.trainingOnlyList = '/docs/trainingOnlyList.txt'
        self.testList = '/docs/TestList.txt'
        self.sequenceLength = sequenceLength
        self.targets = targetFeatures
        self.numTargets = len(targetFeatures)
        self.targetLength = targetLength
        self.inputFeatures = None
        self.splitRatio = splitRatio
        self.dataScaling = dataScaling
        self.scaler = None

    def WalkForwardSplit(self,splitRatio:int):
        hashDfTrain = {}
        hashDfTest = {} 
        for file in os.listdir(self.dfFolderPath):
            if file.endswith(".csv"):
                df = pd.read_csv(file,index_col=[0])
                symbolName = file.split('_')[0]
                dfLength = len(df)
                if len(df) < self.sequenceLength + self.targetLength:
                    self.__writeListFile(self.discardedCompaniesList)
                elif len(df) < self.sequenceLength + 3*self.targetLength:
                    self.__writeListFile(self.trainingOnlyList)
                    splitIndex = self.__findSplitIndex(dfLength)
                    hashDfTrain[symbolName] = df[:splitIndex]
                else:
                    self.__writeListFile(self.testList)
                    splitIndex = self.__findSplitIndex(dfLength)
                    hashDfTrain[symbolName] = df[:splitIndex]
                    hashDfTest[symbolName] = df[(splitIndex-self.sequenceLength):]
        self.__getLabels(hashDfTrain[hashDfTrain.keys([0])]) # get features labels
        self.scaler = self.__computeStandardization(hashDfTrain) # compute scaler over all training data
        x_train_wf, y_train_wf = self.__multivariateDataWFsplitWrapper(hashDfTrain,0)
        assert x_train_wf[-1].shape == torch.Size([self.sequenceLength, len(self.inputFeatures)]), 'Wrong Train Input shape'
        assert y_train_wf[-1].shape == torch.Size([self.targetLength, self.numTargets]), 'Wrong Train Target shape'
        x_validation_wf, y_validation_wf = self.__multivariateDataWFsplitWrapper(hashDfTrain,self.targetLength)
        assert x_validation_wf[-1].shape == torch.Size([self.sequenceLength, len(self.inputFeatures)]), 'Wrong Validation Input shape'
        assert y_validation_wf[-1].shape == torch.Size([self.targetLength, self.numTargets]), 'Wrong Validation Target shape'
        return x_train_wf, y_train_wf, x_validation_wf, y_validation_wf, self.scaler , hashDfTest

    def __computeStandardization(self,hashTrain):
        listDf = [hashTrain[k] for k in hashTrain.keys()]
        concatDf = pd.concat(listDf)
        assert self.inputFeatures != None, 'The columns labels order has not been defined'
        dataToNormalize = concatDf[self.inputFeatures]
        x = dataToNormalize.values #returns a numpy array
        scaler = StandardScaler()
        x_scaled = scaler.fit(x)
        return scaler
    
    def __multivariateDataWFsplitWrapper(self,elementToSplit,startIdx):
        data = []
        labels = []
        if isinstance(elementToSplit,dict):
            for k in elementToSplit.keys():
                inputData,targetData = self.multivariateDataWFsplit(self,k,startIdx, 
                                                None, self.sequenceLength, self.targetLength, 1)
                data.append(inputData)
                labels.append(targetData)
        elif isinstance(elementToSplit,pd.DataFrame):
            inputData,targetData = self.multivariateDataWFsplit(self,k,startIdx, 
                                                None, self.sequenceLength, self.targetLength, 1)
            data.append(inputData)
            labels.append(targetData)
        return torch.tensor(data), torch.tensor(labels).reshape(-1,self.targetLength,self.numTargets)
    
    def multivariateDataWFsplit(self,df,startIndex, end_index, historySize, targetSize, step):
        dataset = df[self.inputFeatures].values
        dfScaled = scaler.transform(df)
        dataset_target = dfScaled[:,0:self.numTargets]
        startIndex = startIndex + historySize
        if end_index is None:
            end_index = len(dataset) - targetSize
        for i in range(startIndex, end_index):
            indices = range(i-historySize, i, step)
        return dataset[indices], [dataset_target[i:i+targetSize]]

    def __writeListFile(self,file,symbolName):
        mode = 'a' if os.path.exists(self.discardedCompaniesList ) else 'w'
        with open(self.discardedCompaniesList , mode) as outfile:
            outfile.write(file.split('_')[0] + '\n')

    def __findSplitIndex(self,dfLength):
        splitIndex = int(dfLength*self.splitRatio) - dfLength%self.targetLength
        return splitIndex

    def __getLabels(self,df):
        columnLabels = df.columns.values
        self.inputFeatures = [self.targets] + [x for x in columnLabels if x not in self.targets]