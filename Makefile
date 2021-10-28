.PHONY: default isvirtualenv

default:
	clear
	@echo "Usage:"
	@echo ""
	@echo "    make format          Formats the entire application."
	@echo "    make test            Tests entire application with pytest."
	@echo ""
	@echo ""

isvirtualenv:
	@if [ -z "$(VIRTUAL_ENV)" ]; \
		then echo "ERROR: Not in a virtualenv." 1>&2; exit 1; fi

format:
	chmod +x scripts/pyformat.sh; scripts/pyformat.sh

test: isvirtualenv
	pytest tests
