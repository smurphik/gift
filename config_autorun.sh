#! /usr/bin/env bash

# Run only with the root privileges
if [ $(id -u) != 0 ]
    then
        echo "Run $ sudo ""$0"
        exit
    fi

# Prepare file gift.service
path_pyt="$(python3 -c 'import sys; print(sys.executable)')"
path_gift="$(whereis gift_server | sed -e 's/^.*[ \t]\([^ \t]*\/gift_server\>\).*$/\1/')"
run_line="$path_pyt"" ""$path_gift"
run_line="$(echo "$run_line" | sed -e 's/\//\\\//g')"
cat gift.service | sed -e 's/gift_server/'"$run_line"'/' > tmp_gift.service
cp tmp_gift.service /lib/systemd/system/gift.service
rm tmp_gift.service

# Reload & enable new service
systemctl daemon-reload
systemctl enable gift.service
systemctl start gift.service
