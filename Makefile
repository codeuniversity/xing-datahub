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
	docker run -p 3000:3000 --net="host" --name producer --rm -d alexmorten/producer
	docker run -p 3001:3001 --net="host" --name consumer --rm -d alexmorten/consumer
	docker run -p 3002:3002 --net="host" --name inserter --rm -d alexmorten/inserter
	docker run -e "token=$(TOKEN)" -p 3003:3003 -p 3004:3004 --net="host" --name endpoint --rm -d alexmorten/query_endpoint

services-down:
	docker stop producer
	docker stop consumer
	docker stop inserter
	docker stop endpoint
