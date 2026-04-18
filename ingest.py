import os, subprocess, logging, chardet

logging.basicConfig(filename='ingestion.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

LOCAL_FILE_PATH = "/mnt/c/Users/pc/Desktop/BDA_A2/yellow_tripdata_2015-01.csv"
HDFS_TARGET_DIR = "/warehouse/raw/nyc_taxi/year=2026/month=04"

def validate_file(filepath):
    logging.info(f"Validating {filepath}")
    if not os.path.exists(filepath): return False

    file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
    logging.info(f"Size: {file_size_mb:.2f} MB")

    result = subprocess.run(['wc', '-l', filepath], capture_output=True, text=True)
    row_count = int(result.stdout.split()[0])
    logging.info(f"Rows: {row_count}")
    return True

def upload_to_hdfs(local_path, hdfs_dir):
    subprocess.run(f"hdfs dfs -mkdir -p {hdfs_dir}", shell=True, check=False)
    upload_cmd = f"hdfs dfs -put -f {local_path} {hdfs_dir}/"
    logging.info(f"Executing: {upload_cmd}")
    return subprocess.run(upload_cmd, shell=True).returncode == 0

if __name__ == "__main__":
    if validate_file(LOCAL_FILE_PATH):
        upload_to_hdfs(LOCAL_FILE_PATH, HDFS_TARGET_DIR)
