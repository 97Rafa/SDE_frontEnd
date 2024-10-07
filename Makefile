build:
	sudo docker compose build

app:
	sudo docker compose up flask_app

db:
	sudo docker compose up -d flask_db

stop:
	sudo docker compose stop