build:
	sudo docker compose build

app:
	sudo docker compose up flask_app

db:
	sudo docker compose up -d flask_db

down:
	sudo docker compose down


all: 
	sudo docker compose up --build

quick: 
	sudo docker compose up


