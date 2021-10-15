.PHONY: all
all: test

.PHONY: init
init: .pip_install_timestamp
.pip_install_timestamp: requirements.txt
	pip install -r requirements.txt
	touch .pip_install_timestamp

.PHONY: test
test: .pip_install_timestamp
	nosetests tests
