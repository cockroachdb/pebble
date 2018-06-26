all: rebuild

.PHONY: rebuild
rebuild: make-db make-table
	./make-table
	rm -fr db-stage-1 && ./make-db 1
	rm -fr db-stage-2 && ./make-db 2
	rm -fr db-stage-3 && ./make-db 3
	rm -fr db-stage-4 && ./make-db 4

make-db: make-db.cc
	g++ -o make-db make-db.cc -lleveldb

make-table: make-table.cc
	g++ -o make-table make-table.cc -lleveldb
