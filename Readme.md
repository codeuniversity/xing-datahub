# First installation
`make env`, `source env/bin/activate` and `make install`

# Generating protobuf source files after changing a .proto file
`make proto`

# Fix "librdkafka" error on Linux
`sudo make librfkafka-linux-install`

# Fix "librdkafka" error on Mac
`sudo make librfkafka-linux-install`

# Run docker infrastructure
- Edit your machine's /etc/hosts file to include this alias:
  `0.0.0.0 quickstart.cloudera`
- ```bash
    docker-compose up
  ```
- To check the docker image for hadoop is working, open a new terminal
  and execute `python sample.py`
