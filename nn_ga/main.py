from keras.callbacks import Callback
from keras.models import Sequential
from keras.layers import Dense, TimeDistributed
from keras.layers import LSTM, GRU, Dropout
from keras.optimizers import Adam, Adagrad, SGD, rmsprop
from keras.callbacks import EarlyStopping, ModelCheckpoint

def model_f():
    opt = Adam()
    model = Sequential()
    model.add(Dense(10, activation="tanh"))
    model.add(Dense(1, activation='sigmoid'))
    model.compile(loss="binary_crossentropy", optimizer=opt, metrics=["accuracy"])