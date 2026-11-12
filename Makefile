.PHONY: up down reset logs ps psql-primary psql-replica test test-local

DOTNET_SDK_IMAGE ?= $(shell sed -n 's/^DOTNET_SDK_IMAGE=//p' $(firstword $(wildcard .env) .env.example))

# local settings are not committed: start from the template
.env:
	cp .env.example .env

up: .env
	docker compose up -d --build

# stops everything and drops the data
down:
	docker compose down -v

# drops all data and starts from scratch
reset: down up

logs:
	docker compose logs -f t1-service

ps:
	docker compose ps

psql-primary:
	docker compose exec pg-primary sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

psql-replica:
	docker compose exec pg-replica sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

# tests without a local .NET SDK (Testcontainers via the Docker socket)
test: .env
	docker run --rm \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v "$(CURDIR)":/repo:ro \
		-v t1-nuget:/root/.nuget/packages \
		--add-host=host.docker.internal:host-gateway \
		-e TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal \
		$(DOTNET_SDK_IMAGE) \
		sh -c 'cp -r /repo /src && cd /src && rm -rf src/*/bin src/*/obj tests/*/bin tests/*/obj && dotnet test --solution T1.sln'

test-local:
	dotnet test --solution T1.sln
