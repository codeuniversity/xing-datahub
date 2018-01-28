
proto:
	protoc --python_out=build Protocol.proto

env:
	python3.6 -m venv env

install:
	pip install -r requirements.txt

librdkafka-linux-install:
	apt install librdkafka-dev

librdkafka-mac-install:
	brew install librdkafka
