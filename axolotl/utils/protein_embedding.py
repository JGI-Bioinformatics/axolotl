"""
# this code is based on https://huggingface.co/Rostlab/prot_t5_xl_bfd

# the model is quite large, one GPU (16GB) is not enough. use a g5.12xlarge with 4 GPUs
# install transformers
!/databricks/python3/bin/python -m pip install transformers

# cached to avoid redownload
model = '/dbfs/FileStore/huggingface/hub/models--Rostlab--prot_t5_xl_bfd/snapshots/7ae1d5c1d148d6c65c7e294cc72807e5b454fdb7'


# For feature extraction we recommend to use the encoder embedding
# embedding has 3 layers, the last hidden state has shape: (2, 8, 1024)
encoder_embedding = embedding[2].cpu().numpy()
"""

import pandas as pd
from Bio import SeqIO
import numpy as np
from transformers import T5Tokenizer, T5Model
import re
import torch


def read_fasta_file(fasta_file):
    sequences = {}
    for record in SeqIO.parse(fasta_file, "fasta"):
        sequences[record.id] = list(str(record.seq).upper())
    return sequences

def process_in_batches(sequences, batch_size, model_path, output_prefix):
    batch_ids, batch_seqs = [], []
    batch_number = 0

    for seq_id, seq in sequences.items():
        batch_ids.append(seq_id)
        batch_seqs.append(seq)

        if len(batch_ids) == batch_size:
            emb, id_list = generate_embeddings(batch_seqs, model_path)
            write_to_parquet(emb, id_list, f"{output_prefix}_batch_{batch_number}.parquet")
            batch_number += 1
            batch_ids, batch_seqs = [], []

    # Process the remaining batch
    if batch_ids:
        emb, id_list = generate_embeddings(batch_seqs, model_path)
        write_to_parquet(emb, id_list, f"{output_prefix}_batch_{batch_number}.parquet")


def generate_embeddings(batch_sequences, model_path):
    """ This function is implemented to generate protein embeddings using the model.
        model_path could be a huggingface model name, or the path to a saved model
        The embedding has a shape: (batch_size, 8, 1024)
    """

    tokenizer = T5Tokenizer.from_pretrained(model_path, do_lower_case=False)
    model = T5Model.from_pretrained(model_path)
    batch_sequences = [re.sub(r"[UZOB]", "X", sequence) for sequence in batch_sequences]
    ids = tokenizer.batch_encode_plus(batch_sequences, add_special_tokens=True, padding=True)

    input_ids = torch.tensor(ids['input_ids'])
    attention_mask = torch.tensor(ids['attention_mask'])

    with torch.no_grad():
        embedding = model(input_ids=input_ids,attention_mask=attention_mask,decoder_input_ids=input_ids)

    encoder_embedding = embedding[2].cpu().numpy()
    return encoder_embedding, batch_sequences
    
def write_to_parquet(embeddings, ids, output_file):
    df = pd.DataFrame(embeddings)
    df.index = ids
    df.to_parquet(output_file)

def main(fasta_file, model_path, output_prefix, batch_size=20):
    sequences = read_fasta_file(fasta_file)
    process_in_batches(sequences, batch_size, model_path, output_prefix)

if __name__ == "__main__":
    fasta_file = "path_to_fasta_file.fasta"
    model_path = "path_to_model"
    output_prefix = "output_prefix"  # Change this to your desired prefix
    batch_size = 20  # You can change this value as needed
    main(fasta_file, model_path, output_prefix, batch_size)