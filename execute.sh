#!/bin/bash -e
source ~/.bashrc
APP_PATH='/home/ec2-user/code/yh-hist-data-ingest'
VIRTUAL_ENV='env'
# Change into data services directory; start Python virtualenv; execute script
cd $APP_PATH

if [ ! -d "$VIRTUAL_ENV" ]; then 
	echo "Note: virtual environment does not exist"
 	pyvenv-3.5 env
fi

source env/bin/activate
pip install -r requirements.txt
echo "Note: virtual environment configured" 

python application.py