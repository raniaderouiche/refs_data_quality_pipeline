import pandas as pd
import torch
from sentence_transformers import SentenceTransformer, util
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.config_loader import load_config

config = load_config()
duplicates_path = config["paths"]["duplicates_path"]

def bert(data):
    df = data.copy()

    df["HIERARCHY"] = df.apply(
        lambda row: "#".join(row["HIERARCHY"].split("#")[:-1]) if row["HIERARCHY"].endswith("#" + str(row["CODE"])) else row["HIERARCHY"],
        axis=1
    )
    # Select relevant columns for similarity check (after comparison with static search)
    # df["text"] = df[["NAME"]].astype(str).agg(" ".join, axis=1)

    df["text"] = df[["NAME", "OFFICIAL_LEVEL_NAME" , "PARENT", "LEVEL_NUMBER"]].astype(str).agg(" - ".join, axis=1)


    # Load DistilBERT model
    model = SentenceTransformer("distilbert-base-nli-stsb-mean-tokens")

    # Encode text as embeddings
    embeddings = model.encode(df["text"].tolist(), convert_to_tensor=True)

    # Compute cosine similarity matrix
    similarity_matrix = util.pytorch_cos_sim(embeddings, embeddings)

    # Set similarity threshold
    threshold = 0.98  # Adjust as needed

    # Identify duplicates
    visited = set()
    duplicates = []

    for i in range(len(df)):
        if i in visited:
            continue
        
        # Find similar rows
        similar_indices = (similarity_matrix[i] > threshold).nonzero(as_tuple=True)[0].tolist()
        
        if len(similar_indices) > 1:
            group = df.iloc[similar_indices]
            duplicates.append(group)
            visited.update(similar_indices)

    final_df = pd.DataFrame()
    for group in duplicates:
        first_occurrence = group.iloc[0:1]  # Keep only the first occurrence
        remaining = group.iloc[1:]  # Store duplicates
        final_df = pd.concat([final_df, first_occurrence, remaining], ignore_index=True)

    final_df.drop(columns=["text"], inplace=True)
    final_df.to_csv(f"../{duplicates_path}", index=False)
    return final_df


if __name__ == "__main__":
    # Example usage
    data = pd.read_csv("data/port_terminals.csv")
    result = bert(data)
    print("File Saved")