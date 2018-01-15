
proto:
	protoc --python_out=build user.proto

env:
	python3.6 -m venv env

install:
	pip install -r requirements.txt
