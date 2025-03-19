import torch
import pandas as pd
from sentence_transformers import SentenceTransformer, util
from sklearn.metrics.pairwise import cosine_similarity

def detect_anomalies():

    # Load dataset
    df = pd.read_csv("C:/Users/Rania/Documents/PFE/refs_data_quality_pipeline/data/DNEXR.REFERENCE.location.csv")

    allowed_values = {"COUNTRY", "CITY", "PORT_TERMINAL"}

    # Replace values not in the allowed set with "REGION"
    df["LEVEL_NAME"] = df["LEVEL_NAME"].apply(lambda x: x if x in allowed_values else "REGION")


    # removing groups (should be treated separtely)
    df = df[df["IS_GROUP"] == False]


    # Values not under a correct hierarchy
    filtered_df = df[~df["HIERARCHY"].str.startswith('ALL')]
    filtered_df


    # Detection using distilBERT
    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

    # Select relevant columns for similarity check
    df["text"] = df[["NAME", "HIERARCHY", "LEVEL_NAME"]].astype(str).agg(" ".join, axis=1)

    # Load DistilBERT model
    model = SentenceTransformer("distilbert-base-nli-stsb-mean-tokens").to(device)

    # Encode text as embeddings
    embeddings = model.encode(df["text"].tolist(), convert_to_tensor=True)

    # Compute cosine similarity matrix
    similarity_matrix = util.pytorch_cos_sim(embeddings, embeddings)

    # Set similarity threshold for anomalies
    anomaly_threshold = 0.27  # Adjust as needed

    # Identify anomalies
    anomalies = []

    for i in range(len(df)):
        # Compute average similarity score for the current row
        avg_similarity = similarity_matrix[i].mean().item()
        
        if avg_similarity < anomaly_threshold:
            anomalies.append(df.iloc[i])

    # Convert anomalies to DataFrame for better visualization
    anomalies_df = pd.DataFrame(anomalies)

    anomalies_df.drop(columns=["text"], inplace=True)


    result_df = pd.concat([filtered_df, anomalies_df], ignore_index=True)
    result_df.to_csv("C:/Users/Rania/Documents/PFE/refs_data_quality_pipeline/data/results/anomalies_detection_result.csv", index=False)

