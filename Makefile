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

format:
	poetry run isort ./interop ./tests
	autoflake \
		--in-place \
		--recursive \
		--remove-all-unused-imports \
		--remove-unused-variables \
		./interop ./tests
	poetry run black ./interop ./tests

test: isvirtualenv
	pytest tests
