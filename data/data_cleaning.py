import pandas as pd
import numpy as np
from scipy import stats
import os
import glob

def clean_data_with_zscore(csv_file_path): #Function to clean data with z-score method
  
    
    # Load the CSV file
    print(f"Loading data from {csv_file_path}...") # Print the path being read
    df = pd.read_csv(csv_file_path) # Load the CSV file
    
    
    original_rows = len(df) # Store original row count
    print(f"Total rows originally: {original_rows}") # Print the original row count
    
    # Identify the numeric columns for z-score analysis
    numeric_columns = ['Temperature (c)', 'Salinity (ppt)', 'ODO mg/L'] # Columns to analyze
    

    missing_columns = [col for col in numeric_columns if col not in df.columns] # Check if all required columns exist
    if missing_columns:
        print(f"Warning: Missing columns: {missing_columns}") # Print the missing columns
        numeric_columns = [col for col in numeric_columns if col in df.columns] # Use available columns only
    
    print(f"Analyzing columns: {numeric_columns}")
    
    # Create a copy for cleaning
    df_clean = df.copy()
    
    # Calculate z-scores for each numeric column
    outlier_mask = pd.Series([False] * len(df_clean))
    
    for column in numeric_columns:
        if column in df_clean.columns:
            # Calculate z-scores for this column
            z_scores = np.abs(stats.zscore(df_clean[column].dropna()))
            
            # Create mask for outliers (|z| > 3.0)
            column_outliers = z_scores > 3.0
            
            # Update the overall outlier mask
            outlier_mask = outlier_mask | column_outliers
            
            print(f"Column '{column}': {column_outliers.sum()} outliers found")
    
    # Count rows to be removed
    rows_to_remove = outlier_mask.sum()
    
    # Remove outliers
    df_cleaned = df_clean[~outlier_mask]
    
    # Calculate remaining rows
    remaining_rows = len(df_cleaned)
    
    print(f"\n--- Data Cleaning Report for {os.path.basename(csv_file_path)} ---") 
    print(f"Total rows originally: {original_rows}") # Print the original row count
    print(f"Rows removed as outliers: {rows_to_remove}") # Print the rows removed as outliers
    print(f"Rows remaining after cleaning: {remaining_rows}") # Print the rows remaining after cleaning
    print(f"Percentage of data retained: {(remaining_rows/original_rows)*100:.2f}%") # Print the percentage of data retained
    
    # Data cleaning complete - no file saving needed
    
    return original_rows, rows_to_remove, remaining_rows, df_cleaned

def process_all_csv_files(data_directory="data"):
 #Function to process all CSV files in the specified directory
    
    csv_pattern = os.path.join(data_directory, "*.csv") # Find all CSV files in the directory
    csv_files = glob.glob(csv_pattern)
    
    # Filter out any cleaned files to avoid processing them
    csv_files = [f for f in csv_files if not f.endswith('_cleaned.csv')]
    
    if not csv_files:
        print(f"No CSV files found in {data_directory}")
        return {}
    
    print(f"Found {len(csv_files)} CSV files to process:")
    for file in csv_files:
        print(f"  - {os.path.basename(file)}")
    
    # Process each CSV file
    results = {}
    total_original = 0
    total_removed = 0
    total_remaining = 0
    
    for csv_file in csv_files:
        filename = os.path.basename(csv_file)
        name_without_ext = os.path.splitext(filename)[0]
        # No output file needed - just processing for reporting
        
        print(f"\n{'='*60}")
        print(f"Processing: {filename}")
        print(f"{'='*60}")
        
        try:
            original, removed, remaining, cleaned_df = clean_data_with_zscore(csv_file)
            
            # Store results
            results[filename] = {
                'original_rows': original,
                'removed_rows': removed,
                'remaining_rows': remaining,
                'retention_rate': (remaining/original)*100,
                'cleaned_file': 'Not saved'
            }
            
            # Add to totals
            total_original += original
            total_removed += removed
            total_remaining += remaining
            
        except Exception as e:
            print(f"Error processing {filename}: {str(e)}")
            results[filename] = {'error': str(e)}
    
    # Print overall summary
    print(f"\n{'='*60}")
    print("OVERALL SUMMARY")
    print(f"{'='*60}")
    print(f"Total files processed: {len(csv_files)}")
    print(f"Total rows originally: {total_original}")
    print(f"Total rows removed as outliers: {total_removed}")
    print(f"Total rows remaining after cleaning: {total_remaining}")
    print(f"Overall data retention rate: {(total_remaining/total_original)*100:.2f}%")
    
    return results

def main():
    #Main function to run the data cleaning process for all CSV files
    
    try:
        # Process all CSV files in the data directory
        results = process_all_csv_files("data")
        
        # Display detailed results for each file
        print(f"\n{'='*60}")
        print("DETAILED RESULTS BY FILE")
        print(f"{'='*60}")
        
        for filename, stats in results.items():
            if 'error' not in stats:
                print(f"\n{filename}:")
                print(f"  Original rows: {stats['original_rows']}")
                print(f"  Removed rows: {stats['removed_rows']}")
                print(f"  Remaining rows: {stats['remaining_rows']}")
                print(f"  Retention rate: {stats['retention_rate']:.2f}%")
                print(f"  Status: {stats['cleaned_file']}")
            else:
                print(f"\n{filename}: ERROR - {stats['error']}")
        
        return results
        
    except Exception as e:
        print(f"Error during data cleaning process: {str(e)}")
        return None

if __name__ == "__main__":
    results = main()