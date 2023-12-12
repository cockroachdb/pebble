all: rebuild

.PHONY: rebuild
rebuild: make-table.go h.txt
	go run ./make-table.go

h.txt: hamlet-act-1.txt
	cat hamlet-act-1.txt | \
		tr '[:upper:]' '[:lower:]' | \
		grep -o -E '\w+' | \
		sort | \
		uniq -c | \
		awk '{printf "%7s %s\n", $$1, $$2}' \
		> h.txt
