#! /usr/bin/env bash

#read -s -p "Create a password for datebase: " db_password
db_password="Qwerty!0"
#echo
text="CREATE DATABASE gift_db CHARACTER SET utf8;
     GRANT ALL PRIVILEGES ON *.* TO 'gift_server'@'localhost' IDENTIFIED BY
     '$db_password';"
echo "Try connect to MySQL as root..."
echo "(your root-password on this host may be required)"
echo "$text" | sudo mysql -u root

echo "[runner]" > ~/.gift.cfg
echo "db_password = $db_password" >> ~/.gift.cfg

echo "Success!"

