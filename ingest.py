import os, subprocess, logging, chardet

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(BASE_DIR, 'ingestion.log')),
        logging.StreamHandler()
    ]
)

LOCAL_FILE_PATH = os.path.join(BASE_DIR, "yellow_tripdata_2015-01.csv")
HDFS_TARGET_DIR = "/warehouse/raw/nyc_taxi/year=2026/month=04"

def validate_file(filepath):
    logging.info(f"Validating {filepath}")
    if not os.path.exists(filepath):
        logging.error(f"File not found: {filepath}")
        return False

    _, ext = os.path.splitext(filepath)
    if ext.lower() != '.csv':
        logging.error(f"Invalid file extension: {ext}. Expected .csv")
        return False
    logging.info(f"File extension validated: {ext}")

    file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
    if file_size_mb == 0:
        logging.error("File is empty (0 bytes)")
        return False
    if file_size_mb < 1:
        logging.warning(f"File is very small: {file_size_mb:.2f} MB")
    logging.info(f"File size: {file_size_mb:.2f} MB")

    with open(filepath, 'rb') as f:
        raw = f.read(100000)
    detected = chardet.detect(raw)
    encoding = detected.get('encoding', 'unknown')
    confidence = detected.get('confidence', 0)
    if confidence < 0.5:
        logging.warning(f"Low encoding confidence: {encoding} ({confidence:.0%})")
    logging.info(f"Detected encoding: {encoding} (confidence: {confidence:.0%})")

    result = subprocess.run(['wc', '-l', filepath], capture_output=True, text=True)
    if result.returncode != 0:
        logging.error(f"Row count failed: {result.stderr.strip()}")
        return False
    row_count = int(result.stdout.split()[0])
    if row_count < 2:
        logging.warning(f"Very few rows: {row_count} (may be header-only)")
    logging.info(f"Row count: {row_count}")

    logging.info("Validation passed")
    return True

def upload_to_hdfs(local_path, hdfs_dir):
    logging.info(f"Creating HDFS directory: {hdfs_dir}")
    mkdir_result = subprocess.run(f"hdfs dfs -mkdir -p {hdfs_dir}", shell=True, capture_output=True, text=True)
    if mkdir_result.returncode != 0:
        logging.error(f"Failed to create HDFS directory: {mkdir_result.stderr.strip()}")
        return False
    logging.info(f"HDFS directory ready: {hdfs_dir}")

    upload_cmd = f"hdfs dfs -put -f {local_path} {hdfs_dir}/"
    logging.info(f"Uploading file: {upload_cmd}")
    upload_result = subprocess.run(upload_cmd, shell=True, capture_output=True, text=True)
    if upload_result.returncode != 0:
        logging.error(f"Upload failed: {upload_result.stderr.strip()}")
        return False
    logging.info(f"Upload successful to {hdfs_dir}")
    return True

if __name__ == "__main__":
    logging.info("=== HDFS Ingestion Pipeline Started ===")
    if validate_file(LOCAL_FILE_PATH):
        if upload_to_hdfs(LOCAL_FILE_PATH, HDFS_TARGET_DIR):
            logging.info("=== Pipeline completed successfully ===")
        else:
            logging.error("=== Pipeline failed at upload step ===")
    else:
        logging.error("=== Pipeline failed at validation step ===")
