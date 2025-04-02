import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def comparison(df1, df2):
    # Identify common rows based on the "CODE" column
    common_rows = pd.merge(df1, df2, on='CODE', how='inner')

    # Identify rows that are in df1 but not in df2
    df1_unique = df1[~df1['CODE'].isin(df2['CODE'])]

    # Identify rows that are in df2 but not in df1
    df2_unique = df2[~df2['CODE'].isin(df1['CODE'])]

    # Print the results
    print("Common Rows:")
    print(len(common_rows))
    print("\nRows found by distilBERT only:")
    print(len(df1_unique))
    print("\nRows found by static search only:")
    print(len(df2_unique))

    # Visual representation
    plt.figure(figsize=(10, 6))

    # Count of common and unique rows
    counts = [len(common_rows), len(df1_unique), len(df2_unique)]
    labels = ['Common Rows', 'Rows found by distilBERT only', 'Rows found by static search only']

    sns.barplot(x=labels, y=counts)
    plt.title('Comparison of DataFrames')
    plt.ylabel('Number of Rows')
    plt.savefig('file:///C:/Users/Rania/Documents/PFE/refs_data_quality_pipeline/data/diagrams/static_bert_comparision.png')

    return common_rows, df1_unique, df2_unique