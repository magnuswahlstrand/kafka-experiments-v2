.PHONY: start-kafka
start-kafka:
	docker-compose up --scale kafka=1
