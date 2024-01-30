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

## Running main.ipynb
It was decided to work in a Jupyter notebook to speed up the execution of the exercise, understanding that it does not comply with the format of an ETL process or data pipeline, however, since it is an exercise with little data and with data unknown to the developer, the Doing an initial Exploratory Data Analyze (EDA) is supposed to be essential to understand the nature and behavior of the data.

1. First you need to select the kernel (venv created in the last step) to run the EDA.ipynb file.
2. Is necessary for the EDA.ipynb and ETL.py to run successfully, that the CodeEx.zip file is in a folder named "data" at the root of the project.