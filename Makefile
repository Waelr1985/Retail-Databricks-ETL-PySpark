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
	pylint \Codes/main.py

all: install format lint test
