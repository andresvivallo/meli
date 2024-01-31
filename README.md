# Meli Excercise
## First steps
1. To run this project you need to first install Python if already don't have <https://www.python.org/downloads/>

2. Second, you need to install pip, to install the necessary dependencies <https://pip.pypa.io/en/stable/installation/>

3. Then, is necessary install virtualenv with the next command: `pip install virtualenv`

## Install Dependencies
Once the last steps are done, you must create a virtual environment and install all the necessary dependencies, these are found in the `requirements.txt` file.

1. Run this command in your terminal to create a virtualenv -> `<PYTHON_VERSION> -m venv </PATH/TO/NEW/VENV>`. Example: `python3.10 -m venv venv`

2. Then, activate this new venv with this command -> `source </PATH/TO/NEW/VENV>/bin/activate`. Example: `source venv/bin/activate`

3. Last, install dependencies with the command -> `pip install -r requirements.txt`

## Running EDA.ipynb
It was decided to work in a Jupyter notebook to carry out an EDA on all the data involved in the exercise, since being an exercise with unknown data, it was very nutritious to be able to give a first inspection of their behavior. In this way, the development of an "ETL" becomes more friendly

1. First you need to select the kernel (venv created in the last step) to run the EDA.ipynb file.
2. Is necessary for the EDA.ipynb to run successfully, that the CodeEx.zip file is in a folder named "data" at the root of the project.
3. With this you can run all cells of EDA.ipynb

In the EDA.html file, you will find the entire EDA.ipynb executed in its entirety, including certain analyses that emerge from the process

## Running main.py
In the main.py file you will find the process to reach the requested results, this process has these main steps defined as functions:
1. unzipping_all_files
2. trf_json_file_to_dataframe
3. join_prints_with_taps
4. calculate_all_metrics

To run this process, here is the step by step:
1. assuming that you already have the dependencies declared in "First steps" and "Install Dependencies", you must activate the environment with the command `source </PATH/TO/NEW/VENV>/activate`
2. Then just run in terminal `python main.py`

## Conclusions and some observations
1. The process on my local computer takes between 5 and 6 minutes to execute, this can clearly be improved but due to the delivery time it could not be made more efficient.
2. Some arbitrary decisions were made within the processing, such as: the weeks were not by calendar-weeks, but rather weeks were considered groupings of 7 days, specifically, the prints of the last week were the last 7 days of the data delivered in prints.json file, this same logic was applied for the 3 weeks prior to these records, that is, 21 days back from the analyzed print record.
3. The df_reslult.csv file was not uploaded to GitHub because it is private data and it is not a good practice to use GitHub as a database of large tabular files, however, it was shared via email for faster review. However, it can be generated by running main.py
4. With the process that is currently in place, it would be easy to incorporate an orchestrator such as Airflow and Prefect 2, to be able to take it to a more friendly process control environment for a development team.




