"""
#variational autoencoders for dimension reduction, clustering, etc

Support for multi-layers, returns an encoder to encode new samples, and a generator to predict based on encodings

## Usage Example:
  from sklearn.model_selection import train_test_split
  from axolotl.utils.vae import vae_nlayers

  # input_data is a pandas dataframe, with its first column is id/labels and the rest of the columns features
  X = input_data.values[:,1:]
  y = input_data.values[:,0]
  x_train, x_test, y_train, y_test = train_test_split(X, y, test_size=.5, random_state=321)
  encoder, generator = vae_nlayers(
  x_train,
  x_test,
  batch_size=256,
  latent_dim=2, # final encoded dimension, 2 can be used for visualization
  intermediate_dim=[256, 64, 16], # 3-layers
  epochs=100,
  epsilon_std=1.0)

  # encoding samples (here we use the validation set)
  # the encoding can be used for visualization, or clustering
  encoded_fams = encoder.predict(x_test)
  
  # decode the new encodings, in case we want to further validate the models
  decoded_fams = generator.predict(encoded_fams)

## Requirements:
This function requires tensorflow, it will run faster on GPUs

To install tensorflow on DataBricks' Spark runtime (cpu, GPU runtimes come with tensorflow isntalled)
```
%%capture
!/databricks/python3/bin/python -m pip install numpy==1.23.0
!/databricks/python3/bin/python -m pip install tensorflow
```

## Limitations:
  - when dataset is large and epochs is large, training is more efficient on GPUs
  
"""

from keras import backend as K
from keras.layers import Input, Dense, Lambda
from keras.models import Model
from keras.callbacks import TensorBoard
from keras import metrics
import numpy as np
from sklearn.model_selection import train_test_split
import pandas as pd

def vae_nlayers(
  x_train,
  x_test,
  batch_size=256,
  latent_dim=2, # final encoded dimensions
  intermediate_dim=[256],
  epochs=20,
  epsilon_std=1.0,
  print_model_structure=False
):
  X = np.vstack((x_train, x_test))
  x_train = x_train.astype('float32') / np.max(X)
  x_test = x_test.astype('float32') / np.max(X)
  x_train = x_train.reshape((len(x_train), np.prod(x_train.shape[1:])))
  x_test = x_test.reshape((len(x_test), np.prod(x_test.shape[1:])))
  original_dim = x_train.shape[1]

  x = Input(shape=(original_dim,))
  if len(intermediate_dim) > 1:
    hi = Dense(intermediate_dim[0], activation='relu')(x)
    for layer in range(len(intermediate_dim)-2):
      hi = Dense(intermediate_dim[layer+1], activation='relu')(hi)
    h = Dense(intermediate_dim[-1], activation='relu')(hi)
  else:
    h = Dense(intermediate_dim[0], activation='relu')(x)
  z_mean = Dense(latent_dim)(h)
  z_log_var = Dense(latent_dim)(h)
  # note that "output_shape" isn't necessary with the TensorFlow backend

  epsilon = K.random_normal(shape=(K.shape(z_mean)[0], latent_dim), mean=0.,
                              stddev=epsilon_std)
  z = z_mean + K.exp(z_log_var / 2) * epsilon

  # we instantiate these layers separately so as to reuse them later
  decoders = []
  decoder_h = Dense(intermediate_dim[-1], activation='relu')
  decoders.append(decoder_h) 
  h_decoded = decoder_h(z)
  if len(intermediate_dim) > 1:
    for layer in range(len(intermediate_dim)-2):
      decoder_h = Dense(intermediate_dim[-1*(layer+1)], activation='relu')
      decoders.append(decoder_h)
      h_decoded = decoder_h(h_decoded)

  decoder_h = Dense(intermediate_dim[0], activation='relu')
  decoders.append(decoder_h)
  h_decoded = decoder_h(h_decoded)
  decoder_mean = Dense(original_dim, activation='sigmoid')
  x_decoded_mean = decoder_mean(h_decoded)

  # instantiate VAE model
  vae = Model(x, x_decoded_mean)

  # Compute VAE loss
  xent_loss = original_dim * metrics.binary_crossentropy(x, x_decoded_mean)
  kl_loss = - 0.5 * K.sum(1 + z_log_var - K.square(z_mean) - K.exp(z_log_var), axis=-1)
  beta = .1
  vae_loss = K.mean(xent_loss + beta*kl_loss)

  vae.add_loss(vae_loss)
  vae.compile(optimizer='rmsprop')
  if print_model_structure:
    vae.summary()

  encoder = Model(x, z_mean)

  # build a digit generator that can sample from the learned distribution
  decoder_input = Input(shape=(latent_dim,))

  decoder_input = Input(shape=(latent_dim,))
  _h_decoded = decoders[0](decoder_input)
  for i in range(1,len(decoders)):
    _h_decoded = decoders[i](_h_decoded)

  _x_decoded_mean = decoder_mean(_h_decoded)
  generator = Model(decoder_input, _x_decoded_mean)

  vae.fit(x_train,
        shuffle=True,
        epochs=epochs,
        batch_size=batch_size,
        validation_data=(x_test, None),
        callbacks=[TensorBoard(log_dir='/tmp/autoencoder/vae')])
  return encoder, generator