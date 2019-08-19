REST API сервис для хранения и анализа данных о потенциальных покупателях магазина подарков.

### Install

В первую очередь, необходимо установить и настроить MySQL сервер. Выполните команды:

    $ sudo apt-get update
    $ sudo apt-get install mysql-server
    $ sudo apt-get install mysql-client
    $ sudo mysql -u root -p

После введения пароля, в консоли MySQL заводим базу данных `gift_db` для сервера и обеспечиваем к ней доступ через пользователя `gift_server`:

    mysql> CREATE DATABASE gift_db CHARACTER SET utf8;
    mysql> GRANT ALL PRIVILEGES ON *.* TO 'gift_server'@'localhost' IDENTIFIED BY 'qwertyqwer';

TODO Скачать/установить, протестировать сам сервер

### Run

Запуск сервера:

    $ ./gift_server.py
