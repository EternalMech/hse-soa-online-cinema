.PHONY: run test demo stop

run:
	docker compose up -d --build --wait

test:
	docker compose --profile test run --rm --build integration

demo:
	docker compose exec -T producer python -m cinema.generator

stop:
	docker compose down
