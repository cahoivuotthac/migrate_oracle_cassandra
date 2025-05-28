up:
	docker-compose up -d 
down: 
	docker-compose down  
restart:
	make up && make down 
build: 
	docker-compose build
