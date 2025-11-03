.PHONY: build up down shell reset-db set-permissions shell postgres-shell

reset-db: down set-permissions
	rm -rfv db-data

build: reset-db
	docker-compose down
	docker-compose build --no-cache

set-permissions:
	mkdir -p db-data
	chmod -R 777 db-data

up: set-permissions
	docker-compose up

down:
	docker-compose down

shell:
	docker-compose exec core bash -c "cd /opt/src; exec bash"

postgres-shell:
	docker-compose exec postgres bash

run:
	docker-compose exec core bash -c "cd /opt/src && python main.py"

run-detached:
	docker-compose exec -d core bash -c "cd /opt/src && python main.py"

unit-tests:
	pytest --disable-warnings -v

test-cov:
	pytest --cov=tests --cov-report=term src/tests/