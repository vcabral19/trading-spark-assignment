.PHONY: install clean test run lint format

install:
	poetry install

test:
	poetry run pytest

clean:
	rm -rf dist
	rm -rf build
	rm -rf *.egg-info

run:
	poetry run spark-submit src/app/main.py

lint:
	poetry run mypy src

format:
	poetry run isort src
	poetry run black src
