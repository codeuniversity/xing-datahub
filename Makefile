dep:
	pip install -r requirements.txt

db-up:
	docker-compose up -d

db-down:
	docker-compose down

pull:
	docker pull alexmorten/producer
	docker pull alexmorten/consumer
	docker pull alexmorten/inserter
	docker pull alexmorten/query_endpoint

services-up:
	docker run -e "token=$(TOKEN)" -p 3000:3000 --net="host" --name producer -d alexmorten/producer
	docker run -p 3001:3001 --net="host" --name consumer -d alexmorten/consumer
	docker run -p 3002:3002 --net="host" --name inserter -d alexmorten/inserter
	docker run -e "token=$(TOKEN)" -p 3003:3003 -p 3004:3004 --net="host" --name endpoint -d alexmorten/query_endpoint

services-down:
	docker stop producer
	docker stop consumer
	docker stop inserter
	docker stop endpoint

clean:
	docker rm producer
	docker rm consumer
	docker rm inserter
	docker rm endpoint
