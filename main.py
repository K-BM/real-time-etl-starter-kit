import yaml
import pandas as pd
import duckdb
import os  # Ensure this import is added

def run_pipeline(config_path):
    # Load YAML
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    
    # Read source data
    if config["pipeline"]["source"]["type"] == "csv":
        df = pd.read_csv(config["pipeline"]["source"]["config"]["path"])
        print("✅ Source data loaded:\n", df.head())
    
    # Apply SQL transformation with DuckDB
    for step in config["pipeline"]["transformation"]:
        query = step["sql"]
        duckdb.register("raw_data", df)  # Register DataFrame as a table
        result = duckdb.sql(query).to_df()
        df = result
        print("✅ Transformation applied:\n", df.head())
    
    # Save to destination
    if config["pipeline"]["destination"]["type"] == "csv":
        output_path = config["pipeline"]["destination"]["config"]["path"]
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        print(f"✅ Data saved to {output_path}")

if __name__ == "__main__":
    run_pipeline("config/pipeline.yaml")