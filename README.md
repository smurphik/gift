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
    mysql> \q

### Server

Установка сервера (если на машине не установлен `git`, можно установить его или скачать содержимое репозитория, используя интерфейс github.com):

    $ git clone https://github.com/smurphik/gift
    $ cd gift
    $ sudo python3 setup.py install clean

Если выполнение последней команды завершилось с ошибкой `[Errno 104] Connection reset by peer -- Some packages may not be found!`, повторяйте эту команду до успешного завершения (сервер PyPi не очень стабилен).

Теперь сервер можно запустить командой:

    $ gift_server

Запущенный сервер, можно протестировать:

    $ ./test.py

N.B. Тестирование оставляет после себя мусорные данные в базе. Для того чтобы почистить базу, нужно остановить сервер и выполнить:

    $ mysql -u gift_server -p
    mysql> `DROP DATABASE gift_db; CREATE DATABASE gift_db CHARACTER SET utf8;
    mysql> \q

### Autorun

Для того, чтобы сервер запускался автоматически при запуске/перезапуске системы, нужно выполнить скрипт:

    $ sudo ./make_autorun.sh

Теперь, при необходимости, можно посмотреть состояние сервера, выполнить его остановку, запуск, перезагрузку при помощи следующих команд:

    $ sudo systemctl status gift.service
    $ sudo systemctl stop gift.service
    $ sudo systemctl start gift.service
    $ sudo systemctl restart gift.service
