CC ?= zig cc -target aarch64-linux-musl
CFLAGS ?= -O2 -Wall

all: ring_daemon

ring_daemon: ring_daemon.c
	$(CC) $(CFLAGS) ring_daemon.c -o ring_daemon -lpthread

clean:
	rm -f ring_daemon
