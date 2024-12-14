.PHONY: default isvirtualenv

default:
	clear
	@echo "Usage:"
	@echo ""
	@echo "    make format          Formats source files."
	@echo "    make test            Runs pytest."
	@echo ""
	@echo ""

isvirtualenv:
	@if [ -z "$(VIRTUAL_ENV)" ]; \
		then echo "ERROR: Not in a virtualenv." 1>&2; exit 1; fi

format: isvirtualenv
	ruff format ./interop ./tests

lint: isvirtualenv
	ruff check --fix ./interop ./tests

test: isvirtualenv
	pytest tests
