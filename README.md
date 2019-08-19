REST API сервис для хранения и анализа данных о потенциальных покупателях магазина подарков.

# Install & Setting

### MySQL

В первую очередь, необходимо установить и настроить MySQL сервер. Выполните команды:

    $ sudo apt-get update
    $ sudo apt-get install mysql-server
    $ sudo apt-get install mysql-client
    $ sudo mysql -u root -p

После введения пароля, в консоли MySQL заводим базу данных `gift_db` для сервера и обеспечиваем к ней доступ через пользователя `gift_server`:

    mysql> CREATE DATABASE gift_db CHARACTER SET utf8;
    mysql> GRANT ALL PRIVILEGES ON *.* TO 'gift_server'@'localhost' IDENTIFIED BY 'Qwerty!0';

### Server

Установка сервера

    $ git clone https://github.com/smurphik/gift
    $ cd gift
    $ sudo python3 setup.py install clean

После установки сервера можно проверить его работоспособность:

    $ ./test.py

Если тесты завершились успешно, то сервер уже можно запустить, выполнив команду:

    $ gift_server

### Autorun

Для того, чтобы сервер запускался автоматически при запуске/перезапуске системы, нужно выполнить скрипт:

    $ sudo ./make_autorun.sh

Теперь, при необходимости, можно посмотреть состояние сервера, выполнить его остановку, запуск, перезашрузку при помощи следующих команд:

    $ sudo systemctl status gift.service
    $ sudo systemctl stop gift.service
    $ sudo systemctl start gift.service
    $ sudo systemctl restart gift.service

