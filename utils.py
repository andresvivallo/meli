import zipfile
import pandas as pd


def unzipping_all_files(
        file: str,
        dest_path: str
    ) -> None:
    print()
    print(f"Unzipping all files from {file} to {dest_path}")

    # Trying to unzip data
    try:
        with zipfile.ZipFile(
            file, "r"
        ) as zip_ref:
            zip_ref.extractall(dest_path)
        print(f"Unzipped successfully!")
    
    # Managing some exception
    except Exception as e:
        print(
            f"Error, please read the point 'Running main.py' " 
            f"on the README file, Exception: {e}"
        )
        raise


def trf_json_file_to_dataframe(
        path_files: str,
        json_config: dict
    ) -> dict[str, pd.DataFrame]:
    print()
    print(
        f"Transforming these json: "
        f"{json_config.keys()} to dataframes"
    )

    # Empty dict to save the result
    result = {}

    # A loop in json files by config
    for json_file in json_config.keys():
        df = pd.read_json(
            f"{path_files}/"
            f"{json_config[json_file]['file']}",
            lines=True
        )

        # Unnesting necessary fields
        for field in json_config[json_file]["nested_fields"]:
            df[f"{field}"] = df[
                f"{json_config[json_file]['nested_column']}"
            ].str.get(f"{field}")
        
        # Saving dataframes into dict
        result[json_file] = df
    
    print(f"Transformed successfully")
    
    return result
        

def join_prints_with_taps(
        df_dict: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    
    # Dropping useless column from prints and taps dataframes
    df_dict["prints"] = df_dict["prints"].drop(
        "event_data", axis=1
    )
    df_dict["taps"] = df_dict["taps"].drop(
        "event_data", axis=1
    )

    # Defining boolean column that represent tap/click
    df_dict["taps"]["tap"] = True

    # Left join between prints and taps dataframe
    prints_with_tap = df_dict["prints"].merge(
        df_dict["taps"],
        on=["day", "user_id", "position", "value_prop"],
        how="left"
    )
    
    # Replacing tap NA with False
    prints_with_tap["tap"].fillna(False, inplace=True)
    
    # Trasnforming column day to datetime type 
    prints_with_tap["day"] = pd.to_datetime(prints_with_tap['day'])

    # Condition for get some data quality info
    if prints_with_tap.tap.sum() == len(df_dict["taps"]):
        per = round(
            len(df_dict['taps']) / len(df_dict['prints']) * 100,
            2
        )
        print(
            f"All taps records matched with some "
            f"register of prints. {len(df_dict['taps'])} "
            f"of {len(df_dict['prints'])} were clicked. "
            f"That's mean a {per} percent"
        )
    else:
        per = round(
            prints_with_tap.tap.sum() / len(df_dict['prints']) * 100,
            2
        )
        print(
            f"Not all taps records matched with some "
            f"register of prints. {prints_with_tap.tap.sum()} "
            f"of {len(df_dict['prints'])} were clicked. "
            f"That's mean a {per} percent"
        )

    # Returning the resulting dataframe
    return prints_with_tap


def read_pays_csv(
        path_file: str
    ) -> pd.DataFrame:
    # Returning dataframe from .csv file
    return pd.read_csv(path_file)
    