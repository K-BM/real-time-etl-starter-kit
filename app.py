import streamlit as st
import pandas as pd
import os
from datetime import datetime

# ------------------------------
# Configure App
# ------------------------------
st.set_page_config(page_title="ETL for Marketing Reports", layout="wide")
st.title("📊 Merge Marketing Reports (Shopify, Facebook Ads, Google Ads)")

# ------------------------------
# Data Source: Upload Files
# ------------------------------
uploaded_files = st.file_uploader(
    "Upload Shopify, Facebook Ads, and Google Ads Reports (CSV/Excel/JSON)",
    type=["csv", "xlsx", "json"],
    accept_multiple_files=True
)

datasets = {}
if uploaded_files:
    for file in uploaded_files:
        try:
            # Read files into DataFrames
            if file.name.endswith('.csv'):
                df = pd.read_csv(file)
            elif file.name.endswith('.xlsx'):
                df = pd.read_excel(file)
            elif file.name.endswith('.json'):
                df = pd.read_json(file)
            else:
                st.error(f"Unsupported format: {file.name}")
                continue
            
            datasets[file.name] = df
            st.success(f"Loaded {file.name} with {len(df)} rows")
        except Exception as e:
            st.error(f"Error loading {file.name}: {str(e)}")

# ------------------------------
# Transformation: Merge Reports (Suffixes Only on Conflicts)
# ------------------------------
if len(datasets) >= 2:
    st.subheader("Merge Reports")
    
    selected_files = st.multiselect(
        "Select datasets to merge", 
        list(datasets.keys()), 
        default=list(datasets.keys())[:3]
    )
    
    # Let users specify merge keys for each dataset
    merge_keys = {}
    for file in selected_files:
        cols = datasets[file].columns.tolist()
        merge_key = st.selectbox(
            f"Select merge key for {file}",
            cols,
            key=f"merge_key_{file}"
        )
        merge_keys[file] = merge_key
    
    # Process files: standardize merge key and track columns
    standardized_key = "merge_key"
    processed_dfs = []
    all_columns = []  # Track non-key columns across all datasets
    
    for file in selected_files:
        df = datasets[file].copy()
        cleaned_name = file.split(".")[0]
        
        # Rename merge key to "merge_key"
        df = df.rename(columns={merge_keys[file]: standardized_key})
        
        # Collect non-key columns for conflict detection
        non_key_columns = [col for col in df.columns if col != standardized_key]
        all_columns.extend(non_key_columns)
        processed_dfs.append((df, cleaned_name, non_key_columns))
    
    # Identify conflicting columns (appear in ≥2 datasets)
    from collections import defaultdict
    column_counts = defaultdict(int)
    for col in all_columns:
        column_counts[col] += 1
    conflicting_columns = {col for col, count in column_counts.items() if count > 1}
    
    # Rename conflicting columns with suffixes
    final_dfs = []
    for df, cleaned_name, non_key_columns in processed_dfs:
        # Add suffix only to conflicting columns
        renamed_columns = {
            col: f"{col}_{cleaned_name}" 
            if col in conflicting_columns 
            else col
            for col in df.columns
        }
        df_renamed = df.rename(columns=renamed_columns)
        final_dfs.append(df_renamed)
    
    # Merge all datasets
    merge_type = st.selectbox("Merge type", ["inner", "left", "outer"])
    
    if st.button("Merge Data"):
        try:
            # Start with first DF
            merged_df = final_dfs[0]
            
            # Merge remaining DFs
            for df in final_dfs[1:]:
                merged_df = pd.merge(
                    merged_df,
                    df,
                    on=standardized_key,
                    how=merge_type
                )
            
            st.session_state.merged_df = merged_df
            st.write("### Merged Data Preview", merged_df.head())
            
        except Exception as e:
            st.error(f"Merge failed: {str(e)}")

# ------------------------------
# Destination: Export
# ------------------------------
if "merged_df" in st.session_state:
    st.subheader("Export Merged Data")
    
    # Export to CSV/Excel
    export_format = st.selectbox("Export format", ["CSV", "Excel"])
    output_filename = f"merged_report_{datetime.now().strftime('%Y%m%d')}.{export_format.lower()}"
    
    if export_format == "CSV":
        csv = st.session_state.merged_df.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name=output_filename,
            mime="text/csv"
        )
    elif export_format == "Excel":
        excel = st.session_state.merged_df.to_excel(index=False)
        st.download_button(
            label="Download Excel",
            data=excel,
            file_name=output_filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )