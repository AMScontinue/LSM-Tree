
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++14 -Wall -g

all: correctness persistence main

correctness: kvstore.o correctness.o SSTable.o SSmanager.o SkipList.o

persistence: kvstore.o persistence.o SSTable.o SSmanager.o SkipList.o

main: kvstore.o main.o SSTable.o SSmanager.o SkipList.o

clean:
	-rm -f correctness persistence *.o
