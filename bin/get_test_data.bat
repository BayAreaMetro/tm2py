REM Copies test data to examples directory or a given directory as an input argument
REM USAGE: get_test_data.bat Optional <directory to put test data>
REM ECHO OFF

SET CWD=%cd%
SET TEST_DATA_LOCATION=https://mtcdrive.box.com/s/3entr016e9teq2wt46x1os3fjqylfoge
REM SET TEST_DATA_NAME=UnionCity
SET DEFAULT_DIRECTORY=examples

ECHO "Retreiving %TEST_DATA_NAME% data from %TEST_DATA_LOCATION%

SET OUTDIR=%1
if "%OUTDIR%"=="" SET OUTDIR=DEFAULT_DIRECTORY
if not exist %OUTDIR% mkdir %OUTDIR%  
CD %OUTDIR%
ECHO "Writing to %CD%"

curl -i -X GET %TEST_DATA_LOCATION% -L -o test_data.zip

CD %CWD%
