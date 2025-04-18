
# Run the model

##	Set up Model Run Directory

1.	Activate your virtual environment

cd to your tm2pyenv type <your_tm2py_env_name>\Scripts\activate

2.	Set your input file paths and expected run paths here:
 tm2py\examples\setup_config.toml
3.	Run setup-model.ipynb
-	In the EMME prompt virtual environment, type jupyter notebook, and Navigate to the file C:\GitHub\tm2py\notebook\setup-model.ipynb
(or Visual studio with the correct virtual environment)
4.	In Jupyter Notebook, open this notebook from the tm2py folder: https://github.com/BayAreaMetro/tm2py/blob/update_openpaths_env/notebooks/setup-model.ipynb
	This notebook references the example setup model config in here: https://github.com/BayAreaMetro/tm2py/blob/update_openpaths_env/examples/setup_config.toml, it copies model inputs and folder structure templates from various places. I added the Box source link of each item in the comments.
	Run this notebook, it should create a clean model folder.
	It also print setup loggings in the model folder.

5.	The only manual change you need to make should be updating the IP Address in CT-RAMP properties files. RunModel.MatrixServerAddress and RunModel.HouseholdServerAddress. you can find them in the model folder:\

"D:\TM2.2.1.3_clean_setup\CTRAMP\runtime\mtctm2.properties"

"D:\TM2.2.1.3_clean_setup\CTRAMP\runtime\mtcpcrm.properties"

"D:\TM2.2.1.3_clean_setup\CTRAMP\runtime\logsum.properties"

6.	Set the model to use warm start demand in the file: scenario_config.toml in your run directory.
Note: I had to update the emmebanks to the latest version before themodel would run.

We had to use a different WalkTransitDriveSkims.xls in the CTRAMP folder

## Run the Model
While still in your virtual environment from step 1 a), navigate to the run folder you set in 2b).
1.	To run the model, in the activate OpenPaths virtual env, type python RunModel.py




