
PROJECTNAME=$(shell basename "$(pwd)")

GOBASE=$(shell pwd)

MIGRATOR_DIR=${GOBASE}/migrations/

MAKEFLAGS += --silent

docker-build-up:
	sudo docker-compose build  && sudo docker-compose up -d