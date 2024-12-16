build:
	docker compose build

app:
	docker compose up flask_app

db:
	docker compose up -d flask_db

down:
	docker compose down


all: 
	docker compose up --build

quick: 
	docker compose up


