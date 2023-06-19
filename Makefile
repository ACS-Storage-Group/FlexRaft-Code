.PHONY: release
release: clean 
	${CMAKE} -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_EXPORT_COMPILE_COMMANDS=true -DLOG=off -DPERF=off
	mv build/compile_commands.json ./
	${CMAKE} --build build -j 4

.PHONY: build
build: clean 
	${CMAKE} -B build -DCMAKE_BUILD_TYPE=Debug -DCMAKE_EXPORT_COMPILE_COMMANDS=true -DLOG=off -DPERF=off
	mv build/compile_commands.json ./
	${CMAKE} --build build -j 4

.PHONY: log
log: clean 
	${CMAKE} -B build -DCMAKE_BUILD_TYPE=Debug -DCMAKE_EXPORT_COMPILE_COMMANDS=true -DLOG=on -DPERF=off
	mv build/compile_commands.json ./
	${CMAKE} --build build -j 4

.PHONY: clean
clean:
	rm -rf build

.PHONY: format
format:
	clang-format -i raft/*.h raft/*.cc
	clang-format -i kv/*.h kv/*.cc
	clang-format -i bench/*.h bench/*.cc

