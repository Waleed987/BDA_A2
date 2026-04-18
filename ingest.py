import os
import subprocess
import logging
import chardet

# Configure logging for successes, warnings, and errors
logging.basicConfig(
    filename='/mnt/c/Users/pc/Desktop/BDA_A2/ingestion.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

LOCAL_FILE_PATH = "/mnt/c/Users/pc/Desktop/BDA_A2/yellow_tripdata_2015-01.csv"
HDFS_TARGET_DIR = "/warehouse/raw/nyc_taxi/year=2026/month=04"

def validate_file(filepath):
    """Pre-upload validation: integrity, encoding, row count."""
    logging.info(f"Starting validation for {filepath}")
    
    if not os.path.exists(filepath):
        logging.error("File does not exist.")
        return False
    if not filepath.endswith('.csv'):
        logging.error("Invalid format. Expected .csv")
        return False
        
    file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
    logging.info(f"File size verified: {file_size_mb:.2f} MB")
    
    with open(filepath, 'rb') as f:
        raw_data = f.read(100000)
        encoding_info = chardet.detect(raw_data)
        logging.info(f"Detected encoding: {encoding_info['encoding']}")
    
    try:
        result = subprocess.run(['wc', '-l', filepath], capture_output=True, text=True)
        row_count = int(result.stdout.split()[0])
        logging.info(f"Row count verified: {row_count} rows.")
        if row_count < 500000:
            logging.warning("Row count is less than 500,000!")
    except Exception as e:
        logging.error(f"Could not verify row count: {e}")

    return True

def upload_to_hdfs(local_path, hdfs_dir):
    """Uploads and organizes files in HDFS."""
    logging.info(f"Preparing to upload to: {hdfs_dir}")
    
    subprocess.run(f"hdfs dfs -mkdir -p {hdfs_dir}", shell=True, check=False)
    upload_cmd = f"hdfs dfs -put -f {local_path} {hdfs_dir}/"
    logging.info(f"Executing: {upload_cmd}")
    
    result = subprocess.run(upload_cmd, shell=True)
    if result.returncode == 0:
        logging.info("Upload to HDFS completed successfully.")
        return True
    else:
        logging.error("Failed to upload to HDFS.")
        return False

def main():
    if validate_file(LOCAL_FILE_PATH):
        upload_to_hdfs(LOCAL_FILE_PATH, HDFS_TARGET_DIR)

if __name__ == "__main__":
    main()