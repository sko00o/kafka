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

run-consumer:
	docker compose exec -it consumer \
		/app/kafka consumer \
			-k kafka:9092 \
			-t test1 \
			-v 2.5.1 \
			--verbose
run-producer:
	docker compose exec -it producer \
		/app/kafka producer \
			-k kafka:9092 \
			-t test1 \
			-v 2.5.1 \
			--sarama --type --idempotent
