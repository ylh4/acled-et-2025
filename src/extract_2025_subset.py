"""
Extract 2025 Subset from Historical Ethiopia Data

Filters the full Ethiopia dataset to extract only 2025 events
and saves cleaned subsets in CSV and Parquet formats.
"""

import sys
from pathlib import Path
import pandas as pd
from datetime import datetime

# Add parent directory to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def extract_2025_subset(
    input_file: Path = None,
    output_dir: Path = None
) -> pd.DataFrame:
    """
    Extract 2025 events from the full Ethiopia dataset.
    
    Parameters
    ----------
    input_file : Path, optional
        Path to full dataset (CSV or Parquet).
        If None, looks for latest ethiopia_*.csv or *.parquet in data_clean/
    output_dir : Path, optional
        Output directory (defaults to project_root/data_clean)
    
    Returns
    -------
    pd.DataFrame
        2025 subset dataset
    """
    # Find input file if not provided
    if input_file is None:
        data_clean_dir = project_root / "data_clean"
        
        # Try to find latest parquet file first
        parquet_files = list(data_clean_dir.glob("ethiopia_*.parquet"))
        if parquet_files:
            input_file = max(parquet_files, key=lambda p: p.stat().st_mtime)
            print(f"Using latest Parquet file: {input_file.name}")
        else:
            # Fall back to CSV
            csv_files = list(data_clean_dir.glob("ethiopia_*.csv"))
            if csv_files:
                input_file = max(csv_files, key=lambda p: p.stat().st_mtime)
                print(f"Using latest CSV file: {input_file.name}")
            else:
                raise FileNotFoundError(
                    f"No Ethiopia dataset found in {data_clean_dir}. "
                    "Please run download_ethiopia.py first."
                )
    
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    # Load data
    print(f"\nLoading data from: {input_file}")
    if input_file.suffix == ".parquet":
        df = pd.read_parquet(input_file)
    else:
        df = pd.read_csv(input_file, low_memory=False)
    
    print(f"Loaded {len(df):,} total records")
    
    # Parse event_date if it's a string
    if "event_date" in df.columns:
        if df["event_date"].dtype == "object":
            df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce")
        
        # Extract year
        df["year"] = df["event_date"].dt.year
        
        # Filter for 2025
        df_2025 = df[df["year"] == 2025].copy()
        
        print(f"Filtered to {len(df_2025):,} records from 2025")
        
        # Drop the temporary year column
        df_2025 = df_2025.drop(columns=["year"], errors="ignore")
        
    elif "year" in df.columns:
        # If year column exists directly
        df_2025 = df[df["year"] == 2025].copy()
        print(f"Filtered to {len(df_2025):,} records from 2025")
    else:
        raise ValueError(
            "Dataset must contain either 'event_date' or 'year' column "
            "to filter 2025 events."
        )
    
    if df_2025.empty:
        print("Warning: No 2025 events found in the dataset!")
        return pd.DataFrame()
    
    # Set up output directory
    if output_dir is None:
        output_dir = project_root / "data_clean"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate output filenames
    csv_file = output_dir / "ethiopia_2025.csv"
    parquet_file = output_dir / "ethiopia_2025.parquet"
    
    # Save CSV
    print(f"\nSaving 2025 subset to CSV: {csv_file}")
    df_2025.to_csv(csv_file, index=False)
    print(f"✓ CSV saved ({len(df_2025)} rows)")
    
    # Save Parquet
    print(f"Saving 2025 subset to Parquet: {parquet_file}")
    df_2025.to_parquet(parquet_file, index=False, engine="pyarrow")
    print(f"✓ Parquet saved ({len(df_2025)} rows)")
    
    # Print summary
    print("\n" + "="*60)
    print("2025 SUBSET SUMMARY")
    print("="*60)
    print(f"Total 2025 records: {len(df_2025):,}")
    if "event_date" in df_2025.columns:
        print(f"Date range: {df_2025['event_date'].min()} to {df_2025['event_date'].max()}")
    if "fatalities" in df_2025.columns:
        total_fatalities = df_2025["fatalities"].sum()
        print(f"Total fatalities: {total_fatalities:,}")
    if "admin1" in df_2025.columns:
        regions = df_2025["admin1"].nunique()
        print(f"Regions with events: {regions}")
    print(f"\nFiles saved:")
    print(f"  CSV: {csv_file}")
    print(f"  Parquet: {parquet_file}")
    print("="*60)
    
    return df_2025


def main():
    """Main execution function."""
    print("="*60)
    print("EXTRACT ETHIOPIA 2025 SUBSET")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    try:
        df_2025 = extract_2025_subset()
        
        if df_2025.empty:
            print("\n✗ No 2025 data found. Exiting.")
            return
        
        print(f"\n✓ Extraction complete!")
        print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"\n✗ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

