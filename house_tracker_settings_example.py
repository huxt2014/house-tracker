
database = {
    'drivername': 'mysql+pymysql',
    'host': 'localhost',
    'port': 3306,
    'username': 'test',
    'password': 'test',
    'database': 'house_tracker',
}

log_file = "/tmp/log.txt"

# for email
email_list = "example@163.com"
smtp = {
    "host": "smtp.163.com",
    "user": "example@163.com",
    "password": "password"
}

# for gunicorn
bind = ["127.0.0.1:8080"]
worker_class = "gthread"
threads = 4
accesslog = "/tmp/web.log"
errorlog = "/tmp/web.log"
