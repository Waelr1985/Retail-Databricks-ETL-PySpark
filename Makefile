#Adding this to install all the packages in requirements.txt - I have not included versions of the individual packages in the requirements file
install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

#Adding this to format code using black
format:	
	black \Codes/*.py 

test:
	python -m pytest \Codes/Test_*.py

lint:
	flake8 Codes --max-line-length=120 --ignore=E302,E305,E231,E241,E702,F401

all: install format lint test
