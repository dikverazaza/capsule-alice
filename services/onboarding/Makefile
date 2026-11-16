CC ?= zig cc -target aarch64-linux-musl
CFLAGS ?= -O2 -Wall

all: capsule_onboarding

capsule_onboarding: onboarding_daemon.c
	$(CC) $(CFLAGS) onboarding_daemon.c -o capsule_onboarding -lpthread

clean:
	rm -f capsule_onboarding
