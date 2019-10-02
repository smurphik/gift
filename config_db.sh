#! /usr/bin/env bash

#read -s -p "Create a password for datebase: " db_password
db_password="Qwerty?0"
sudo -i -u postgres psql -c "CREATE USER gift_server PASSWORD '$db_password';"
sudo -i -u postgres psql -c "CREATE DATABASE gift_db;"

#echo "[runner]" > ~/.gift.cfg
#echo "db_password = $db_password" >> ~/.gift.cfg

echo "Success!"

