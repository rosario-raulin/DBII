
HEADER_FILES := $(wildcard */*.hpp)

all: main

main: base_column.cpp  main.cpp  unittest.cpp ${HEADER_FILES}
	g++ -Wall -Wextra -Weffc++ -Werror -I. main.cpp base_column.cpp unittest.cpp -o main -lboost_serialization-mt

run:
	./main

documentation:
	cd doc; doxygen doxygen.conf

view_documentation:
	firefox doc/documentation/index.htm

release:
	rm -rf Release db2_programming_project_SoSe2013 db2_programming_project_SoSe2013.tgz
	svn export . Release/
	rm -rf Release/.gitignore
	mv Release db2_programming_project_SoSe2013
	tar -cvzf db2_programming_project_SoSe2013.tgz db2_programming_project_SoSe2013


