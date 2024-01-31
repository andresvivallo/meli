import os
import zipfile
import pandas as pd

from datetime import timedelta


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
    

def calculate_all_metrics(
        prints_with_taps: pd.DataFrame,
        path_pays_csv: str
) -> dict[str, pd.DataFrame]:
    print()
    print(
        f"Calculating all necessary metrics, "
        f"this process take a few minutes.."
    )
    # Necessary value for printed process logic
    process_percent_printed = 0
    # Read pays.csv
    df_pays = pd.read_csv(path_pays_csv)
    # To datetime
    df_pays["pay_date"] = pd.to_datetime(df_pays["pay_date"])
    
    # Considering the last week as the last 7 days recorded in the data
    start_date_of_last_week = (
        prints_with_taps.day.max() - timedelta(days = 6)
    )
    
    # Filtering only the last week prints
    prints_last_week = prints_with_taps.loc[
        prints_with_taps.day >= start_date_of_last_week
    ].reset_index()
    
    # Creating empty dataframes for save results
    three_lw_prints_result = pd.DataFrame()
    three_lw_taps_result = pd.DataFrame()
    three_lw_pays_counts_result = pd.DataFrame()
    three_lw_pays_total_result = pd.DataFrame()

    # Working by users
    users_ids = list(prints_last_week.user_id.unique())
    for i in range(len(users_ids)):
        user_id = users_ids[i]
        process_percent = round(i/(len(users_ids)-1)*100)
        if process_percent in list(range(2, 102, 2)):
            if process_percent != process_percent_printed:
                print(f"{process_percent}% of the process done!")
                process_percent_printed = process_percent
            
                
        
        # Getting unique days of specific user
        days = list(
            prints_last_week[
                prints_last_week.user_id == user_id
            ].day.unique()
        )
        # Loop into unique days of the user
        for k in range(len(days)):
            day = days[k]
            
            # Defining window of analyze, considering the 
            # last 3 weeks (21 days) before the objective row
            end_date = day - timedelta(days=1)
            start_date = (
                end_date - timedelta(days=20)
            )
            
            # Filtering prints with the window of analyze and user_id
            mask = (
                prints_with_taps.day >= start_date
            ) & (
                prints_with_taps.day <= end_date
            ) & (
                prints_with_taps.user_id == user_id
            )
            
            # Filtering pays with the window of analyze and user_id
            mask_pays = (
                df_pays.pay_date >= start_date
            ) & (
                df_pays.pay_date <= end_date
            ) & (
                df_pays.user_id == user_id
            )

            # Applying filter in prints and pays DF
            prints_with_taps_filter = prints_with_taps.loc[mask]
            df_pays_filter = df_pays.loc[mask_pays]
            
            # Counting prints by value_prop
            counts_prints = prints_with_taps_filter.groupby(
                "value_prop"
            ).count().reset_index()[
                ["value_prop", "day"]
            ].rename(columns={"day": "count"})
            # adding useful info
            counts_prints["user_id"] = user_id
            counts_prints["day"] = day

            # Counting taps by value_prop
            counts_taps = prints_with_taps_filter[
                prints_with_taps_filter["tap"]
            ].groupby(
                "value_prop"
            ).count().reset_index()[
                ["value_prop", "day"]
            ].rename(columns={"day": "count"})
            # adding useful info
            counts_taps["user_id"] = user_id
            counts_taps["day"] = day

            # Counting pays by value_prop
            counts_pays = df_pays_filter.groupby(
                "value_prop"
            ).count().reset_index()[
                ["value_prop", "pay_date"]
            ].rename(columns={"pay_date": "count"})
            # adding useful info
            counts_pays["user_id"] = user_id
            counts_pays["day"] = day

            # Sum total pays by value_prop
            total_pays = df_pays_filter.groupby(
                "value_prop"
            ).sum("total").reset_index()[
                ["value_prop", "total"]
            ].rename(columns={"pay_date": "count"})
            # adding useful info
            total_pays["user_id"] = user_id
            total_pays["day"] = day
            
            # Saving results
            three_lw_prints_result = pd.concat(
                [
                    three_lw_prints_result,
                    counts_prints
                ]
            )
            three_lw_taps_result = pd.concat(
                [
                    three_lw_taps_result,
                    counts_taps
                ]
            )
            three_lw_pays_counts_result = pd.concat(
                [
                    three_lw_pays_counts_result,
                    counts_pays
                ]
            )
            three_lw_pays_total_result = pd.concat(
                [
                    three_lw_pays_total_result,
                    total_pays
                ]
            )
    
    # Long to wide and rename columns of all results
    prefix_prints = "l3wprints"
    prefix_taps = "l3wtaps"
    prefix_count_pays = "l3wcountpays"
    prefix_total_pays = "l3wtotalpays"

    # Prints 
    three_lw_prints_result_wide = three_lw_prints_result.pivot(
        index=["day", "user_id"],
        columns=["value_prop"],
        values="count"
    ).reset_index().rename(
        columns={
            "cellphone_recharge": f"{prefix_prints}_cellphone_recharge",
            "credits_consumer" : f"{prefix_prints}_credits_consumer",
            "link_cobro" : f"{prefix_prints}_link_cobro",
            "point" : f"{prefix_prints}_point",
            "prepaid" : f"{prefix_prints}_prepaid",
            "send_money" : f"{prefix_prints}_send_money",
            "transport" : f"{prefix_prints}_transport",
        }
    )

    # Taps
    three_lw_taps_result_wide = three_lw_taps_result.pivot(
        index=["day", "user_id"],
        columns=["value_prop"],
        values="count"
    ).reset_index().rename(
        columns={
            "cellphone_recharge": f"{prefix_taps}_cellphone_recharge",
            "credits_consumer" : f"{prefix_taps}_credits_consumer",
            "link_cobro" : f"{prefix_taps}_link_cobro",
            "point" : f"{prefix_taps}_point",
            "prepaid" : f"{prefix_taps}_prepaid",
            "send_money" : f"{prefix_taps}_send_money",
            "transport" : f"{prefix_taps}_transport",
        }
    )
     
    # Count Pays
    three_lw_pays_counts_result_wide = three_lw_pays_counts_result.pivot(
        index=["day", "user_id"],
        columns=["value_prop"],
        values="count"
    ).reset_index().rename(
        columns={
            "cellphone_recharge": f"{prefix_count_pays}_cellphone_recharge",
            "credits_consumer" : f"{prefix_count_pays}_credits_consumer",
            "link_cobro" : f"{prefix_count_pays}_link_cobro",
            "point" : f"{prefix_count_pays}_point",
            "prepaid" : f"{prefix_count_pays}_prepaid",
            "send_money" : f"{prefix_count_pays}_send_money",
            "transport" : f"{prefix_count_pays}_transport",
        }
    )

    # Total Pays
    three_lw_pays_total_result_wide = three_lw_pays_total_result.pivot(
        index=["day", "user_id"],
        columns=["value_prop"],
        values="total"
    ).reset_index().rename(
        columns={
            "cellphone_recharge": f"{prefix_total_pays}_cellphone_recharge",
            "credits_consumer" : f"{prefix_total_pays}_credits_consumer",
            "link_cobro" : f"{prefix_total_pays}_link_cobro",
            "point" : f"{prefix_total_pays}_point",
            "prepaid" : f"{prefix_total_pays}_prepaid",
            "send_money" : f"{prefix_total_pays}_send_money",
            "transport" : f"{prefix_total_pays}_transport",
        }
    )

    # Fill NaN with 0
    three_lw_prints_result_wide.fillna(0, inplace=True)
    three_lw_taps_result_wide.fillna(0, inplace=True)
    three_lw_pays_counts_result_wide.fillna(0, inplace=True)
    three_lw_pays_total_result_wide.fillna(0, inplace=True)

    # Merging all dataframes
    prints_last_week_with_l3w_data = prints_last_week.merge(
        three_lw_prints_result_wide,
        on=["day", "user_id"],
        how="left"
    ).merge(
        three_lw_taps_result_wide,
        on=["day", "user_id"],
        how="left"
    ).merge(
        three_lw_pays_counts_result_wide,
        on=["day", "user_id"],
        how="left"
    ).merge(
        three_lw_pays_total_result_wide,
        on=["day", "user_id"],
        how="left"
    )

    # Fill NaN in df_result and save results in CSV
    prints_last_week_with_l3w_data.fillna(0, inplace=True)
    df_result = prints_last_week_with_l3w_data
    df_result_describe = df_result.describe()
    df_result.to_csv(
        f"{os.getcwd()}/df_result.csv"
    )
    prints_last_week_with_l3w_data.describe().to_csv(
        f"{os.getcwd()}/df_result_describe.csv"
    )

    # Create dict to return results
    result = {
        "df_result": df_result,
        "df_result_describe": df_result_describe,
        "df_result_file": f"{os.getcwd()}/df_result.csv",
        "df_result_describe_file": f"{os.getcwd()}/df_result_describe.csv"
    }

    return result
    