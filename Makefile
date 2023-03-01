all: demo

demo: up watch down

up:
	docker compose up -d --build --remove-orphans
watch:
	sleep 10
	docker compose logs --tail=10 producer
	docker compose logs --tail=10 consumer
down:
	docker compose down -v --remove-orphans

.PHONY: all demo up watch down
