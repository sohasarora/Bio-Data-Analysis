import pandas as pd
import numpy as np

# Load and clean biological dataset (example: gene expression data)
def clean_data(file_path):
    data = pd.read_csv(file_path)
    # Handle missing values
    data = data.dropna()  # Drop rows with missing values
    # Standardize column names
    data.columns = data.columns.str.strip().str.lower().str.replace(' ', '_')
    return data

# Analyze dataset to uncover key findings
def analyze_data(data):
    # Example: Find top 10 genes with highest expression levels
    top_genes = data.groupby('gene')['expression_level'].mean().nlargest(10)
    return top_genes

# Integrate data from UCSC Genome Browser and NCBI
def integrate_genome_data(ucsc_file, ncbi_file):
    ucsc_data = pd.read_csv(ucsc_file)
    ncbi_data = pd.read_csv(ncbi_file)
    # Merge datasets on a common identifier (e.g., gene ID)
    integrated_data = pd.merge(ucsc_data, ncbi_data, on='gene_id', how='inner')
    return integrated_data

# Automate data processing pipeline
def automate_pipeline(input_files, ucsc_file, ncbi_file):
    combined_results = []
    for file_path in input_files:
        data = clean_data(file_path)
        findings = analyze_data(data)
        combined_results.append(findings)

    # Integrate external datasets
    integrated_data = integrate_genome_data(ucsc_file, ncbi_file)
    return combined_results, integrated_data

# Example usage
if __name__ == "__main__":
    input_files = ["gene_data1.csv", "gene_data2.csv"]
    ucsc_file = "ucsc_genome.csv"
    ncbi_file = "ncbi_data.csv"

    results, integrated = automate_pipeline(input_files, ucsc_file, ncbi_file)

    # Output results
    print("Top gene findings:")
    for i, res in enumerate(results):
        print(f"Dataset {i + 1}:")
        print(res)

    print("\nIntegrated genome data:")
    print(integrated.head())
