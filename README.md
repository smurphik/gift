REST API сервис для хранения и анализа данных о потенциальных покупателях магазина подарков.

### Install

В первую очередь, необходимо установить MySQL сервер:

    $ sudo apt-get update
    $ sudo apt-get install mysql-server
    $ sudo apt-get install mysql-client

Устанавливаем сервер подарков (если на машине не установлен `git`, можно установить его или скачать содержимое репозитория, используя интерфейс github.com):

    $ git clone https://github.com/smurphik/gift
    $ cd gift
    $ sudo python3 setup.py install clean

Если выполнение последней команды завершилось с ошибкой `[Errno 104] Connection reset by peer -- Some packages may not be found!`, повторяйте эту команду до успешного завершения (сервер PyPi не очень стабилен).

### Setting & Testing

Теперь нужно создать базу данных с именем `gift_db` и завести пользователя `gift_server`, чтобы сервер мог через него обращаться к базе. Это можно сделать вызовом скрипта:

    $ ./config_db.sh

В процессе его работы потребуется придумать и ввести пароль для пользователя `gift_db`.

Теперь сервер можно запустить командой:

    $ gift_server

Запущенный сервер, можно протестировать:

    $ ./test.py

N.B. Тестирование оставляет после себя мусорные данные в базе. Для того чтобы почистить базу, нужно остановить сервер и выполнить:

    $ mysql -u gift_server -p
    mysql> DROP DATABASE gift_db; CREATE DATABASE gift_db CHARACTER SET utf8;
    mysql> \q

### Autorun

Для того, чтобы сервер запускался автоматически при запуске/перезапуске системы, нужно выполнить скрипт:

    $ sudo ./config_autorun.sh

Теперь, при необходимости, можно посмотреть состояние сервера, выполнить его остановку, запуск, перезагрузку при помощи следующих команд:

    $ sudo systemctl status gift.service
    $ sudo systemctl stop gift.service
    $ sudo systemctl start gift.service
    $ sudo systemctl restart gift.service
