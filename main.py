import os
import time
import warnings

from utils import (
    unzipping_all_files,
    trf_json_file_to_dataframe,
    join_prints_with_taps,
    calculate_all_metrics
)
from config import JSON_CONFIG

ZIP_FILE = "CodeEx.zip"
DEST_PATH = "tmp"
PAY_FILE = "pays.csv"


if __name__ == "__main__":
    start_time = time.time()
    warnings.filterwarnings("ignore")
    wd = os.getcwd()

    # Unzipping files from .zip
    unzipping_all_files(
        file=f"{wd}/data/{ZIP_FILE}",
        dest_path=f"{wd}/{DEST_PATH}/"
    )
    
    # Transform json files to python dataframes
    df_dict = trf_json_file_to_dataframe(
        path_files=f"{wd}/{DEST_PATH}/{ZIP_FILE.replace('.zip', '')}",
        json_config=JSON_CONFIG
    )
    
    # Transform json files to python dataframes
    prints_with_taps = join_prints_with_taps(
        df_dict=df_dict
    )

    final_result = calculate_all_metrics(
        prints_with_taps=prints_with_taps,
        path_pays_csv=(
            f"{wd}/{DEST_PATH}/"
            f"{ZIP_FILE.replace('.zip', '')}/{PAY_FILE}"
        )
    )

    # Print process time and file results paths
    process_time = round(
        (time.time() - start_time)/60,
        2
    )
    print()
    print(
        f"All process was completed successfully in "
        f"{process_time} minutes, "
        f"you can see the result dataframe in the file: "
        f"{final_result.get('df_result_file')}, and "
        f"a descriptive table in the following file: "
        f"{final_result.get('df_result_describe_file')}"
    )
