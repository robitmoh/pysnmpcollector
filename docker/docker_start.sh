#!/bin/sh
echo "Start pysnmpcollector"


#export ENV_HOSTS="${ENV_HOSTS:-''}"
#/usr/bin/envsubst < /infpyng/docker/config/config.env >/infpyng/config/config.toml

exec /usr/local/bin/python ./pysnmpcollector.py

