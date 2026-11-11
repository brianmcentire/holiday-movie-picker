PORT ?= 8000
PID_FILE := .dev-server.pid
LOG_FILE := .dev-server.log
DEPS := .venv/.dev-deps

.PHONY: dev dev-stop scores

## make dev: serve the site at http://localhost:8000 in the background, reloading the browser on changes
dev: $(DEPS)
	@if [ -f $(PID_FILE) ] && kill -0 $$(cat $(PID_FILE)) 2>/dev/null; then \
		echo "Already running at http://localhost:$(PORT) (make dev-stop to stop it)"; \
	else \
		nohup .venv/bin/python dev-server.py $(PORT) > $(LOG_FILE) 2>&1 & echo $$! > $(PID_FILE); \
		sleep 1; \
		if kill -0 $$(cat $(PID_FILE)) 2>/dev/null; then \
			echo "Serving at http://localhost:$(PORT), reloading on changes (make dev-stop to stop it)"; \
		else \
			echo "The dev server didn't start:"; cat $(LOG_FILE); rm -f $(PID_FILE); exit 1; \
		fi; \
	fi

## make dev-stop: stop the dev server
dev-stop:
	@if [ -f $(PID_FILE) ] && kill $$(cat $(PID_FILE)) 2>/dev/null; then \
		echo "Stopped the dev server"; \
	else \
		echo "The dev server wasn't running"; \
	fi
	@rm -f $(PID_FILE)

## make scores: fetch IMDb and Rotten Tomatoes scores from MDBList into scores.json (needs MDBLIST_API_KEY in .env)
scores: $(DEPS)
	@.venv/bin/python scores-builder.py

# Creates the venv and installs dependencies, again whenever the requirements files change
$(DEPS): requirements.txt requirements-dev.txt
	@test -d .venv || python3 -m venv .venv
	@.venv/bin/pip install -q --disable-pip-version-check -r requirements.txt -r requirements-dev.txt
	@touch $@
