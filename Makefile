install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

test:
	python -m pytest -vv --cov=main --cov=mylib tests/

format:	
	black mylib/*.py tests/*.py src/*.py

lint:
	#disable comment to test speed
	#pylint --disable=R,C --ignore-patterns=tests/*.py mylib/*.py
	#ruff linting is 10-100X faster than pylint
	ruff check mylib/*.py

container-lint:
	docker run --rm -i hadolint/hadolint < Dockerfile

refactor: format lint

deploy:
	#deploy goes here
		
all: install lint test format deploy
