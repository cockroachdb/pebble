all: rebuild

.PHONY: rebuild
rebuild: make-db.go
	for stage in 1 2 3 4; do \
  	rm -fr db-stage-$$stage && \
		go run make-db.go $$stage; \
	done
