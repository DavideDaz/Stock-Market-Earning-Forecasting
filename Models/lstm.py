import torch
import torch.nn as nn
import torch.nn.functional as F

class LSTM(nn.Module):

    def __init__(self, input_dim, hidden_dim, batch_size, output_dim,output_seq,
                    num_layers, sequence_length, conditions_embed_sizes):
        super(LSTM, self).__init__()
        self.input_dim = input_dim
        self.hidden_dim = hidden_dim
        self.batch_size = batch_size
        self.num_layers = num_layers
        self.sequence_length = sequence_length
        self.output_dim = output_dim
        self.output_seq = output_seq
        self.output_dim_seq = self.output_dim * self.output_seq
        self.conditions_embed_size = conditions_embed_sizes


        # Define the LSTM layer
        self.lstm = nn.LSTM(self.input_dim, self.hidden_dim, self.num_layers,batch_first=True)

        # Define the output layer
        self.linear = nn.Linear(self.hidden_dim, self.output_dim_seq)

        #Define conditional linear layer
        self.linear_conditional = nn.ModuleList([nn.Linear(num_classes, output_size) \
                                        for num_classes, output_size in conditions_embed_sizes])
        #Define linear layer for initial state
        self.linear_initial_state = nn.Linear(sum([x[1] for x in self.conditions_embed_size]),self.hidden_dim)
    
    def init_hidden(self,initial_conditions):
        # This is what we'll initialise our hidden state as
        return (torch.cat([initial_conditions]*self.num_layers).reshape(self.num_layers,initial_conditions.shape[0],self.hidden_dim),
                torch.zeros(self.num_layers, initial_conditions.shape[0], self.hidden_dim))

    def forward(self, input,conditional_input):
        sequenceInput,conditionsInput = input.float(),[conditional_input]
        conditions = [l(conditionsInput[i]) for i, l in enumerate(self.linear_conditional)]
        conditions_flat = torch.cat(conditions)
        self.initial_conditions = self.linear_initial_state(conditions_flat)
        self.hc = self.init_hidden(self.initial_conditions)
        lstm_out, hidden = self.lstm(sequenceInput,self.hc)
        output = self.linear(lstm_out[:,-1])
        y_pred = output.reshape((-1,self.output_seq,self.output_dim))
        return y_pred
