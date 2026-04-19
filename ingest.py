import os, subprocess, logging, chardet

# base directory of this script (so paths work regardless of where we run it from)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# setting up logging (both file + console)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(BASE_DIR, 'ingestion.log')),  # log file
        logging.StreamHandler()  # print logs on terminal
    ]
)

# local CSV file path
LOCAL_CSV_PATH = os.path.join(BASE_DIR, "yellow_tripdata_2015-01.csv")

# HDFS target directory (partitioned by year/month)
HDFS_DEST_DIR = "/warehouse/raw/nyc_taxi/year=2026/month=04"


def validate_file(file_path):
    # basic validation before uploading
    logging.info(f"Validating {file_path}")

    # check if file exists
    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        return False

    # check extension (must be CSV)
    _, extension = os.path.splitext(file_path)
    if extension.lower() != '.csv':
        logging.error(f"Invalid file extension: {extension}. Expected .csv")
        return False
    logging.info(f"File extension validated: {extension}")

    # check file size
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    if file_size_mb == 0:
        logging.error("File is empty (0 bytes)")
        return False
    if file_size_mb < 1:
        logging.warning(f"File is very small: {file_size_mb:.2f} MB")
    logging.info(f"File size: {file_size_mb:.2f} MB")

    # detect encoding using chardet (just to avoid weird encoding issues later)
    with open(file_path, 'rb') as f:
        raw_sample = f.read(100000)  # read first ~100KB
    detected_info = chardet.detect(raw_sample)
    encoding = detected_info.get('encoding', 'unknown')
    confidence = detected_info.get('confidence', 0)

    if confidence < 0.5:
        logging.warning(f"Low encoding confidence: {encoding} ({confidence:.0%})")
    logging.info(f"Detected encoding: {encoding} (confidence: {confidence:.0%})")

    # count rows using wc -l (faster than python for big files)
    wc_result = subprocess.run(['wc', '-l', file_path], capture_output=True, text=True)
    if wc_result.returncode != 0:
        logging.error(f"Row count failed: {wc_result.stderr.strip()}")
        return False

    row_count = int(wc_result.stdout.split()[0])
    if row_count < 2:
        logging.warning(f"Very few rows: {row_count} (maybe only header)")
    logging.info(f"Row count: {row_count}")

    logging.info("Validation passed")
    return True


def upload_to_hdfs(local_path, hdfs_path):
    # create HDFS directory if it doesn't exist
    logging.info(f"Creating HDFS directory: {hdfs_path}")
    mkdir_cmd = f"hdfs dfs -mkdir -p {hdfs_path}"
    mkdir_result = subprocess.run(mkdir_cmd, shell=True, capture_output=True, text=True)

    if mkdir_result.returncode != 0:
        logging.error(f"Failed to create HDFS directory: {mkdir_result.stderr.strip()}")
        return False
    logging.info(f"HDFS directory ready: {hdfs_path}")

    # upload file to HDFS
    upload_cmd = f"hdfs dfs -put -f {local_path} {hdfs_path}/"
    logging.info(f"Uploading file: {upload_cmd}")
    upload_result = subprocess.run(upload_cmd, shell=True, capture_output=True, text=True)

    if upload_result.returncode != 0:
        logging.error(f"Upload failed: {upload_result.stderr.strip()}")
        return False

    logging.info(f"Upload successful to {hdfs_path}")
    return True


if __name__ == "__main__":
    logging.info("=== HDFS Ingestion Pipeline Started ===")

    # step 1: validate file
    if validate_file(LOCAL_CSV_PATH):
        # step 2: upload if validation passes
        if upload_to_hdfs(LOCAL_CSV_PATH, HDFS_DEST_DIR):
            logging.info("=== Pipeline completed successfully ===")
        else:
            logging.error("=== Pipeline failed at upload step ===")
    else:
        logging.error("=== Pipeline failed at validation step ===")