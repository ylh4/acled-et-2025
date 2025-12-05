"""
Download ACLED Data for Ethiopia (2018 to Current Date)

Downloads all ACLED events for Ethiopia from 2018 to the current date,
handles pagination, writes raw CSVs, concatenates, removes duplicates,
and saves final versions in both CSV and Parquet formats.
"""

import sys
from pathlib import Path
import pandas as pd
from datetime import datetime
import time

# Add parent directory to path to import acled_client
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.acled_client import ACLEDClient


def download_ethiopia_historical(
    start_year: int = 2018,
    end_year: int = None,
    output_dir: Path = None,
    limit: int = 1000,
    save_raw: bool = True
) -> pd.DataFrame:
    """
    Download all ACLED data for Ethiopia from start_year to end_year.
    
    Parameters
    ----------
    start_year : int, default 2018
        Starting year for data download
    end_year : int, optional
        Ending year (defaults to current year)
    output_dir : Path, optional
        Output directory (defaults to project_root/data_raw)
    limit : int, default 1000
        Records per API request
    save_raw : bool, default True
        Whether to save individual page CSVs
    
    Returns
    -------
    pd.DataFrame
        Combined and deduplicated dataset
    """
    # Set default end year to current year
    if end_year is None:
        end_year = datetime.now().year
    
    # Set up paths
    if output_dir is None:
        output_dir = project_root / "data_raw"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Initialize client
    print("Initializing ACLED API client...")
    try:
        client = ACLEDClient()
        print("✓ Client initialized successfully")
    except Exception as e:
        print(f"✗ Failed to initialize client: {str(e)}")
        raise
    
    # Test connection
    print("\nTesting API connection...")
    if not client.test_connection():
        raise ConnectionError("Failed to connect to ACLED API. Check your credentials.")
    print("✓ Connection successful\n")
    
    # Prepare query parameters
    print(f"Downloading Ethiopia data from {start_year} to {end_year}...")
    print(f"Using ISO code 231 (Ethiopia)\n")
    
    # Build year filter
    if start_year == end_year:
        year_filter = str(start_year)
        year_where = None
    else:
        year_filter = f"{start_year}|{end_year}"
        year_where = "BETWEEN"
    
    # Download all pages
    all_data = []
    page_num = 1
    
    try:
        all_data = client.get_all_pages(
            endpoint="acled/read",
            iso=231,  # Ethiopia ISO code
            year=year_filter,
            year_where=year_where,
            limit=limit,
            progress=True
        )
        
        print(f"\n✓ Successfully downloaded {len(all_data)} records")
        
    except Exception as e:
        print(f"\n✗ Error during download: {str(e)}")
        raise
    
    # Convert to DataFrame
    if not all_data:
        print("Warning: No data retrieved!")
        return pd.DataFrame()
    
    df = pd.DataFrame(all_data)
    print(f"\nDataFrame created: {len(df)} rows, {len(df.columns)} columns")
    
    # Save raw data if requested
    if save_raw:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_file = output_dir / f"ethiopia_raw_{start_year}_{end_year}_{timestamp}.csv"
        df.to_csv(raw_file, index=False)
        print(f"✓ Raw data saved to: {raw_file}")
    
    # Remove duplicates
    print("\nRemoving duplicates...")
    initial_count = len(df)
    
    # Check for event_id or data_id column for deduplication
    if "data_id" in df.columns:
        df = df.drop_duplicates(subset=["data_id"], keep="first")
        print(f"  Deduplicated by 'data_id': {initial_count} → {len(df)} records")
    elif "event_id" in df.columns:
        df = df.drop_duplicates(subset=["event_id"], keep="first")
        print(f"  Deduplicated by 'event_id': {initial_count} → {len(df)} records")
    else:
        # Fallback: remove all duplicate rows
        df = df.drop_duplicates(keep="first")
        print(f"  Deduplicated by all columns: {initial_count} → {len(df)} records")
    
    print(f"✓ Removed {initial_count - len(df)} duplicate records")
    
    return df


def save_final_datasets(
    df: pd.DataFrame,
    start_year: int,
    end_year: int,
    output_dir: Path = None
) -> tuple:
    """
    Save final datasets in CSV and Parquet formats.
    
    Parameters
    ----------
    df : pd.DataFrame
        Cleaned dataset
    start_year : int
        Start year for filename
    end_year : int
        End year for filename
    output_dir : Path, optional
        Output directory (defaults to project_root/data_clean)
    
    Returns
    -------
    tuple
        Paths to CSV and Parquet files
    """
    if output_dir is None:
        output_dir = project_root / "data_clean"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate filenames
    csv_file = output_dir / f"ethiopia_{start_year}_{end_year}.csv"
    parquet_file = output_dir / f"ethiopia_{start_year}_{end_year}.parquet"
    
    # Save CSV
    print(f"\nSaving CSV to: {csv_file}")
    df.to_csv(csv_file, index=False)
    print(f"✓ CSV saved ({len(df)} rows)")
    
    # Save Parquet
    print(f"Saving Parquet to: {parquet_file}")
    df.to_parquet(parquet_file, index=False, engine="pyarrow")
    print(f"✓ Parquet saved ({len(df)} rows)")
    
    # Print summary statistics
    print("\n" + "="*60)
    print("DOWNLOAD SUMMARY")
    print("="*60)
    print(f"Time period: {start_year} - {end_year}")
    print(f"Total records: {len(df):,}")
    print(f"Date range in data: {df['event_date'].min()} to {df['event_date'].max()}")
    if "fatalities" in df.columns:
        total_fatalities = df["fatalities"].sum()
        print(f"Total fatalities: {total_fatalities:,}")
    print(f"\nFiles saved:")
    print(f"  CSV: {csv_file}")
    print(f"  Parquet: {parquet_file}")
    print("="*60)
    
    return csv_file, parquet_file


def main():
    """Main execution function."""
    print("="*60)
    print("ACLED ETHIOPIA DATA DOWNLOAD")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Configuration
    START_YEAR = 2018
    END_YEAR = datetime.now().year
    
    try:
        # Download data
        df = download_ethiopia_historical(
            start_year=START_YEAR,
            end_year=END_YEAR,
            save_raw=True
        )
        
        if df.empty:
            print("\n✗ No data retrieved. Exiting.")
            return
        
        # Save final datasets
        csv_path, parquet_path = save_final_datasets(
            df=df,
            start_year=START_YEAR,
            end_year=END_YEAR
        )
        
        print(f"\n✓ Download complete!")
        print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except KeyboardInterrupt:
        print("\n\nDownload interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

