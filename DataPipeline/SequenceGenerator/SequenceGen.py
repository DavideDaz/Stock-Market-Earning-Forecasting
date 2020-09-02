import os
import pandas as pd 
import torch
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler

class SequenceBuilder():
    def __init__(self, dfFolderPath, sequenceLength, targetLength, targetFeatures:list,splitRatio,dataScaling:str):
        self.ROOT_DIR = '/Users/davideconcu/Documents/Stock Analysis'
        self.dfFolderPath = dfFolderPath
        self.discardedCompaniesList = self.ROOT_DIR + '/DataPipeline/SequenceGenerator/docs/discardedList.txt'
        self.trainingOnlyList = self.ROOT_DIR + '/DataPipeline/SequenceGenerator/docs/trainingOnlyList.txt'
        self.testList = self.ROOT_DIR + '/DataPipeline/SequenceGenerator/docs/TestList.txt'
        self.sequenceLength = sequenceLength
        self.targets = targetFeatures
        self.numTargets = len(targetFeatures)
        self.targetLength = targetLength
        self.inputFeatures = None
        self.splitRatio = splitRatio
        self.dataScaling = dataScaling
        self.scaler = None

    def WalkForwardSplit(self):
        hashDfTrain = {}
        hashDfTest = {} 
        for file in os.listdir(self.ROOT_DIR+self.dfFolderPath):
            if file.endswith(".csv"):
                df = pd.read_csv(self.ROOT_DIR+self.dfFolderPath+file,index_col=[0])
                symbolName = file.split('_')[0]
                dfLength = len(df)
                if len(df) < self.sequenceLength + self.targetLength:
                    self.__writeListFile(self.discardedCompaniesList,symbolName)
                elif len(df) < self.sequenceLength + 3*self.targetLength:
                    self.__writeListFile(self.trainingOnlyList,symbolName)
                    hashDfTrain[symbolName] = df
                else:
                    self.__writeListFile(self.testList,symbolName)
                    splitIndex = self.__findSplitIndex(dfLength)
                    hashDfTrain[symbolName] = df[:splitIndex]
                    hashDfTest[symbolName] = df[(splitIndex-self.sequenceLength):]
        self.__getLabels(hashDfTrain[list(hashDfTrain.keys())[0]]) # get features labels
        self.scaler = self.__computeStandardization(hashDfTrain) # compute scaler over all training data
        x_train_wf, y_train_wf = self.__multivariateDataWFsplitWrapper(hashDfTrain,0,self.scaler)
        assert x_train_wf[-1].shape == torch.Size([self.sequenceLength, len(self.inputFeatures)]), 'Wrong Train Input shape'
        assert y_train_wf[-1].shape == torch.Size([self.targetLength, self.numTargets]), 'Wrong Train Target shape'
        x_validation_wf, y_validation_wf = self.__multivariateDataWFsplitWrapper(hashDfTrain,self.targetLength,self.scaler)
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
    
    def __multivariateDataWFsplitWrapper(self,elementToSplit,startIdx,scaler):
        if isinstance(elementToSplit,dict):
            inputData,targetData = [],[]
            for k in elementToSplit.keys():
                inputDataK,targetDataK = self.multivariateDataWFsplit(elementToSplit[k],startIdx, 
                                                None, self.sequenceLength, self.targetLength, 1,scaler)
                inputData += inputDataK
                targetData += targetDataK
        elif isinstance(elementToSplit,pd.DataFrame):
            inputData,targetData = self.multivariateDataWFsplit(elementToSplit[k],startIdx, 
                                                None, self.sequenceLength, self.targetLength, 1,scaler)
        return torch.tensor(inputData).reshape(-1,self.sequenceLength,len(self.inputFeatures)), torch.tensor(targetData).reshape(-1,self.targetLength,self.numTargets)
    
    def multivariateDataWFsplit(self,df,startIndex, end_index, historySize, targetSize, step,scaler):
        data = []
        labels = []
        dataset = df[self.inputFeatures].values
        dfScaled = scaler.transform(dataset)
        dataset_target = dfScaled[:,0:self.numTargets]
        startIndex = startIndex + historySize
        if end_index is None:
            end_index = len(dataset) - targetSize
        for i in range(startIndex, end_index+1,2*targetSize):
            indices = range(i-historySize, i, step)
            data.append(dfScaled[indices])
            labels.append([dataset_target[i:i+targetSize]])
        return data, labels

    def __writeListFile(self,file,symbolName):
        mode = 'a' if os.path.exists(file ) else 'w'
        with open(file , mode) as outfile:
            outfile.write(symbolName + '\n')

    def __findSplitIndex(self,dfLength):
        splitIndex = int(dfLength*self.splitRatio) - dfLength%self.targetLength
        return splitIndex+1

    def __getLabels(self,df):
        columnLabels = df.columns.values
        self.inputFeatures = self.targets + [x for x in columnLabels if x not in self.targets]