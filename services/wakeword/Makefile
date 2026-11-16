CXX ?= zig c++ -target aarch64-linux-musl
CXXFLAGS ?= -O2 -Wall -std=c++11

all: wakewordd

wakewordd: wakewordd.cc
	$(CXX) $(CXXFLAGS) wakewordd.cc -o wakewordd -lm

clean:
	rm -f wakewordd
