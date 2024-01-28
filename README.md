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