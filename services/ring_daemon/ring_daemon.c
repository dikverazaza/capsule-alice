#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <stdint.h>
#include <math.h>
#include <signal.h>
#include <time.h>
#include <errno.h>

#define RING_WRITE 0x40107a01
#define NUM_LEDS 12
#define RING_CHANNELS (NUM_LEDS * 3)
#define FIFO_PATH "/tmp/ring.fifo"

struct frame_req {
    uint32_t start;
    uint32_t count;
    uint8_t *data;
};

static int g_ring_fd = -1;
static volatile sig_atomic_t g_running = 1;

static void log_ring_to(const char *path, const char *line) {
    int fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0) return;
    char buf[256];
    int n = snprintf(buf, sizeof(buf), "%ld %s\n", (long)time(NULL), line);
    if (n > 0) {
        if (n >= (int)sizeof(buf)) n = (int)sizeof(buf) - 1;
        ssize_t ignored = write(fd, buf, (size_t)n);
        (void)ignored;
    }
    close(fd);
}

static void log_ring_command(const char *line) {
    log_ring_to("/tmp/ring_commands.log", line);
}

static void log_ring_watchdog(const char *line) {
    log_ring_to("/tmp/ring_watchdog.log", line);
}

void set_ring(uint8_t *buf) {
    if (g_ring_fd < 0) return;
    struct frame_req req = {
        .start = 0,
        .count = RING_CHANNELS,
        .data = buf
    };
    ioctl(g_ring_fd, RING_WRITE, &req);
}

void clear_ring(void) {
    uint8_t buf[RING_CHANNELS];
    memset(buf, 0, sizeof(buf));
    set_ring(buf);
}

// Reset front screen LEDs once on startup, then leave them completely untouched
void reset_screen_leds_once(void) {
    char path[64];
    for (int i = 1; i <= 30; i++) {
        snprintf(path, sizeof(path), "/sys/class/leds/clock-led%d/brightness", i);
        int fd = open(path, O_WRONLY);
        if (fd >= 0) {
            pwrite(fd, "0", 1, 0);
            close(fd);
        }
    }
}

void sig_handler(int sig) {
    (void)sig;
    g_running = 0;
}

enum RingState {
    RING_OFF = 0,
    RING_WAKE,
    RING_LISTEN,
    RING_THINK,
    RING_SPEAK,
    RING_MUSIC,
    RING_MUTE,
    RING_NIGHT,
    RING_VOLUME,
    RING_ALERT
};

static int ring_state_timeout_sec(enum RingState s) {
    switch (s) {
        case RING_WAKE:   return 3;    // мигновенный отклик на wake word
        case RING_LISTEN: return 15;   // COMMAND_LISTEN_WINDOW = 10 с + запас
        case RING_THINK:  return 25;   // Vins до 9 с + TTS до 15 с
        case RING_SPEAK:  return 30;   // длинная реплика, но не вечно
        case RING_MUSIC:  return 120;  // музыка может играть долго, но сработает предохранитель
        case RING_ALERT:  return 70;   // будильник гасится через 60 с
        case RING_VOLUME: return 4;    // собственный g_volume_until = 2 с
        default:          return 0;    // OFF/NIGHT/MUTE — штатные постоянные состояния
    }
}

static int g_volume_level = 35;
static time_t g_volume_until = 0;

enum NightMode {
    NIGHT_STATIC = 0,
    NIGHT_BREATHING,
    NIGHT_GRADIENT
};

// Top ring nightlight state (Driver byte order: G, R, B)
static int g_night_active = 0;
static uint8_t g_night_r = 255;
static uint8_t g_night_g = 150;
static uint8_t g_night_b = 30;
static float g_night_brightness = 0.7f;
static enum NightMode g_night_mode = NIGHT_STATIC;
static time_t g_pause_until = 0;
static int g_mic_muted = 0;

// Top ring nightlight renderer
void render_night_ring(uint8_t *ring_buf, float elapsed_sec) {
    if (g_night_mode == NIGHT_STATIC) {
        uint8_t g = (uint8_t)(g_night_g * g_night_brightness);
        uint8_t r = (uint8_t)(g_night_r * g_night_brightness);
        uint8_t b = (uint8_t)(g_night_b * g_night_brightness);
        for (int i = 0; i < NUM_LEDS; i++) {
            ring_buf[i * 3 + 0] = g;
            ring_buf[i * 3 + 1] = r;
            ring_buf[i * 3 + 2] = b;
        }
    } else if (g_night_mode == NIGHT_BREATHING) {
        float phase = elapsed_sec * 1.5f;
        float factor = (sinf(phase) + 1.0f) * 0.4f + 0.2f; // 20%..100%
        uint8_t g = (uint8_t)(g_night_g * g_night_brightness * factor);
        uint8_t r = (uint8_t)(g_night_r * g_night_brightness * factor);
        uint8_t b = (uint8_t)(g_night_b * g_night_brightness * factor);
        for (int i = 0; i < NUM_LEDS; i++) {
            ring_buf[i * 3 + 0] = g;
            ring_buf[i * 3 + 1] = r;
            ring_buf[i * 3 + 2] = b;
        }
    } else if (g_night_mode == NIGHT_GRADIENT) {
        float t = elapsed_sec * 0.3f;
        for (int i = 0; i < NUM_LEDS; i++) {
            float hue = fmodf(t + (float)i / (float)NUM_LEDS, 1.0f);
            float h6 = hue * 6.0f;
            int hi = (int)h6 % 6;
            float f = h6 - (float)hi;
            float q = 1.0f - f;
            float r_f, g_f, b_f;
            switch (hi) {
                case 0: r_f = 1.0f; g_f = f;    b_f = 0.0f; break;
                case 1: r_f = q;    g_f = 1.0f; b_f = 0.0f; break;
                case 2: r_f = 0.0f; g_f = 1.0f; b_f = f;    break;
                case 3: r_f = 0.0f; g_f = q;    b_f = 1.0f; break;
                case 4: r_f = f;    g_f = 0.0f; b_f = 1.0f; break;
                default: r_f = 1.0f; g_f = 0.0f; b_f = q;   break;
            }
            ring_buf[i * 3 + 0] = (uint8_t)(g_f * 255.0f * g_night_brightness);
            ring_buf[i * 3 + 1] = (uint8_t)(r_f * 255.0f * g_night_brightness);
            ring_buf[i * 3 + 2] = (uint8_t)(b_f * 255.0f * g_night_brightness);
        }
    }
}

int main(int argc, char **argv) {
    (void)argc; (void)argv;
    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);
    signal(SIGPIPE, SIG_IGN);

    g_ring_fd = open("/dev/topleds", O_RDWR);
    if (g_ring_fd < 0) {
        perror("open /dev/topleds");
    }

    // Clear front screen LEDs once on boot, then leave them alone (no screen control)
    reset_screen_leds_once();

    unlink(FIFO_PATH);
    mkfifo(FIFO_PATH, 0666);
    int fifo_fd = open(FIFO_PATH, O_RDWR | O_NONBLOCK);
    if (fifo_fd < 0) {
        perror("open fifo");
    }

    enum RingState current_ring_state = RING_OFF;
    time_t state_since = time(NULL);
    clear_ring();

    uint8_t ring_buf[RING_CHANNELS];
    int step = 0;
    char read_buf[256];

    struct timespec start_ts, current_ts;
    clock_gettime(CLOCK_MONOTONIC, &start_ts);

    while (g_running) {
        clock_gettime(CLOCK_MONOTONIC, &current_ts);
        float elapsed_sec = (float)(current_ts.tv_sec - start_ts.tv_sec) +
                            (float)(current_ts.tv_nsec - start_ts.tv_nsec) * 1e-9f;

        // 0. Auto-reconnect FIFO if unlinked or inode changed
        struct stat st_fd, st_path;
        if (fifo_fd < 0 || fstat(fifo_fd, &st_fd) != 0 || st_fd.st_nlink == 0 ||
            stat(FIFO_PATH, &st_path) != 0 || st_fd.st_ino != st_path.st_ino) {
            if (fifo_fd >= 0) close(fifo_fd);
            unlink(FIFO_PATH);
            mkfifo(FIFO_PATH, 0666);
            fifo_fd = open(FIFO_PATH, O_RDWR | O_NONBLOCK);
        }

        // 1. Process incoming commands from FIFO
        if (fifo_fd >= 0) {
            ssize_t n = read(fifo_fd, read_buf, sizeof(read_buf) - 1);
            if (n > 0) {
                read_buf[n] = '\0';
                char *saveptr;
                char *line = strtok_r(read_buf, "\r\n", &saveptr);
                while (line) {
                    while (*line == ' ') line++;
                    char *end = line + strlen(line) - 1;
                    while (end > line && *end == ' ') *end-- = '\0';
                    if (*line) log_ring_command(line);

                    // --- RING COMMANDS ---
                    if (strncmp(line, "off", 3) == 0 || strncmp(line, "idle", 4) == 0 || strncmp(line, "stop", 4) == 0) {
                        if (g_mic_muted) {
                            current_ring_state = RING_MUTE;
                        } else if (g_night_active) {
                            current_ring_state = RING_NIGHT;
                            step = 0;
                        } else {
                            current_ring_state = RING_OFF;
                            step = 0;
                            clear_ring();
                        }
                    } else if (strncmp(line, "wake", 4) == 0) {
                        current_ring_state = RING_WAKE;
                        step = 0;
                    } else if (strncmp(line, "listen", 6) == 0) {
                        current_ring_state = RING_LISTEN;
                        step = 0;
                    } else if (strncmp(line, "think", 5) == 0) {
                        current_ring_state = RING_THINK;
                        step = 0;
                    } else if (strncmp(line, "speak", 5) == 0) {
                        current_ring_state = RING_SPEAK;
                        step = 0;
                    } else if (strncmp(line, "music", 5) == 0) {
                        current_ring_state = (g_night_active) ? RING_NIGHT : RING_MUSIC;
                        step = 0;
                    } else if (strncmp(line, "mute", 4) == 0) {
                        g_mic_muted = 1;
                        current_ring_state = RING_MUTE;
                        step = 0;
                    } else if (strncmp(line, "unmute", 6) == 0) {
                        g_mic_muted = 0;
                        current_ring_state = (g_night_active) ? RING_NIGHT : RING_OFF;
                        step = 0;
                        if (!g_night_active) clear_ring();
                    } else if (strncmp(line, "alert", 5) == 0) {
                        current_ring_state = RING_ALERT;
                        step = 0;
                    }
                    // --- NIGHTLIGHT COMMANDS ---
                    else if (strncmp(line, "night_on", 8) == 0) {
                        g_night_active = 1;
                        if (!g_mic_muted) {
                            current_ring_state = RING_NIGHT;
                        }
                    } else if (strncmp(line, "night_off", 9) == 0) {
                        g_night_active = 0;
                        if (g_mic_muted) {
                            current_ring_state = RING_MUTE;
                        } else {
                            current_ring_state = RING_OFF;
                            clear_ring();
                        }
                    } else if (strncmp(line, "night_color", 11) == 0) {
                        int r, g, b;
                        float br = 0.7f;
                        if (sscanf(line + 11, "%d %d %d %f", &r, &g, &b, &br) >= 3) {
                            g_night_r = (uint8_t)r;
                            g_night_g = (uint8_t)g;
                            g_night_b = (uint8_t)b;
                            g_night_brightness = br;
                            g_night_active = 1;
                            current_ring_state = RING_NIGHT;
                        }
                    } else if (strncmp(line, "night_mode", 10) == 0) {
                        char mode_str[32] = {0};
                        if (sscanf(line + 10, "%31s", mode_str) == 1) {
                            if (strcmp(mode_str, "breathing") == 0) g_night_mode = NIGHT_BREATHING;
                            else if (strcmp(mode_str, "gradient") == 0) g_night_mode = NIGHT_GRADIENT;
                            else g_night_mode = NIGHT_STATIC;
                            g_night_active = 1;
                            current_ring_state = RING_NIGHT;
                        }
                    } else if (strncmp(line, "night_brightness", 16) == 0) {
                        float br = 0.7f;
                        if (sscanf(line + 16, "%f", &br) == 1) {
                            g_night_brightness = (br < 0.05f) ? 0.05f : ((br > 1.0f) ? 1.0f : br);
                        }
                    }
                    // --- VOLUME / PAUSE COMMANDS ---
                    else if (strncmp(line, "volume", 6) == 0) {
                        int v = 35;
                        if (sscanf(line + 6, "%d", &v) == 1) {
                            g_volume_level = (v < 0) ? 0 : ((v > 100) ? 100 : v);
                        }
                        current_ring_state = RING_VOLUME;
                        g_volume_until = time(NULL) + 2;
                    } else if (strncmp(line, "pause", 5) == 0) {
                        g_pause_until = time(NULL) + 2;
                    }
                    // Any screen_* commands are safely ignored (no screen control)

                    state_since = time(NULL);
                    line = strtok_r(NULL, "\r\n", &saveptr);
                }
            }
        }

        if (time(NULL) < g_pause_until) {
            usleep(40000);
            continue;
        }

        // 1.5 Hard watchdog: no animation may stay lit forever.
        // Alice may crash, hang in Vins/TTS, or simply forget to send "off".
        {
            time_t now = time(NULL);
            int limit = ring_state_timeout_sec(current_ring_state);
            if (limit > 0 && now - state_since >= limit) {
                char msg[128];
                snprintf(msg, sizeof(msg), "watchdog: state=%d stuck %ld s -> off",
                         (int)current_ring_state, (long)(now - state_since));
                log_ring_watchdog(msg);
                current_ring_state = g_mic_muted ? RING_MUTE
                                   : (g_night_active ? RING_NIGHT : RING_OFF);
                step = 0;
                state_since = now;
                clear_ring();
            }
        }

        // 2. Render top LED ring
        switch (current_ring_state) {
            case RING_OFF:
                if (step == 0) {
                    clear_ring();
                    step = 1;
                }
                break;

            case RING_ALERT: {
                // Яркая плавная янтарная пульсация таймера / будильника (R=255, G=90, B=0)
                float phase = (float)step * 0.22f;
                float b = 0.15f + 0.85f * (0.5f * (1.0f + sinf(phase)));
                for (int i = 0; i < NUM_LEDS; i++) {
                    ring_buf[i * 3 + 0] = (uint8_t)(90.0f * b);  // G
                    ring_buf[i * 3 + 1] = (uint8_t)(255.0f * b); // R
                    ring_buf[i * 3 + 2] = 0;                     // B
                }
                set_ring(ring_buf);
                step++;
                break;
            }

            case RING_VOLUME: {
                if (time(NULL) >= g_volume_until) {
                    if (g_mic_muted) {
                        current_ring_state = RING_MUTE;
                    } else {
                        current_ring_state = (g_night_active) ? RING_NIGHT : RING_OFF;
                    }
                    step = 0;
                    clear_ring();
                    break;
                }
                int count = (NUM_LEDS * g_volume_level + 50) / 100;
                if (count > NUM_LEDS) count = NUM_LEDS;
                for (int i = 0; i < NUM_LEDS; i++) {
                    if (i < count) {
                        ring_buf[i * 3 + 0] = 120; // G
                        ring_buf[i * 3 + 1] = 60;  // R
                        ring_buf[i * 3 + 2] = 255; // B (cyan/purple Alice glow)
                    } else {
                        ring_buf[i * 3 + 0] = 0;
                        ring_buf[i * 3 + 1] = 0;
                        ring_buf[i * 3 + 2] = 0;
                    }
                }
                set_ring(ring_buf);
                break;
            }

            case RING_NIGHT:
                render_night_ring(ring_buf, elapsed_sec);
                set_ring(ring_buf);
                break;

            case RING_MUTE:
                for (int i = 0; i < NUM_LEDS; i++) {
                    ring_buf[i * 3 + 0] = 0;   // G
                    ring_buf[i * 3 + 1] = 255; // R
                    ring_buf[i * 3 + 2] = 0;   // B
                }
                set_ring(ring_buf);
                break;

            case RING_WAKE:
                // Solid bright Alice purple: G=15, R=107, B=255 (#6B0FFF)
                for (int i = 0; i < NUM_LEDS; i++) {
                    ring_buf[i * 3 + 0] = 15;
                    ring_buf[i * 3 + 1] = 107;
                    ring_buf[i * 3 + 2] = 255;
                }
                set_ring(ring_buf);
                step++;
                break;

            case RING_LISTEN: {
                // Алиса слушает: спокойное, глубокое королевское фиолетовое дыхание
                // Все 12 светодиодов светятся мягким благородным индиго-фиолетовым (R=130, G=15, B=255)
                // с плавным синусоидальным дыханием (период ~2.2 с).
                float phase = (float)step * 0.075f;
                float breath = 0.25f + 0.75f * (0.5f * (1.0f + sinf(phase)));
                uint8_t g_val = (uint8_t)(15.0f * breath);
                uint8_t r_val = (uint8_t)(130.0f * breath);
                uint8_t b_val = (uint8_t)(255.0f * breath);

                for (int i = 0; i < NUM_LEDS; i++) {
                    ring_buf[i * 3 + 0] = g_val; // G
                    ring_buf[i * 3 + 1] = r_val; // R
                    ring_buf[i * 3 + 2] = b_val; // B
                }
                set_ring(ring_buf);
                step++;
                break;
            }

            case RING_THINK: {
                // Алиса думает: вращающаяся неоновая фиолетово-бирюзовая комета со шлейфом
                int head = (step * 2) % NUM_LEDS;
                for (int i = 0; i < NUM_LEDS; i++) {
                    int dist = (head - i + NUM_LEDS) % NUM_LEDS;
                    float b = 0.0f;
                    if (dist == 0) b = 1.0f;
                    else if (dist == 1) b = 0.65f;
                    else if (dist == 2) b = 0.35f;
                    else if (dist == 3) b = 0.15f;
                    else if (dist == 4) b = 0.05f;
                    else b = 0.0f;

                    ring_buf[i * 3 + 0] = (uint8_t)(35 * b);  // G
                    ring_buf[i * 3 + 1] = (uint8_t)(130 * b); // R
                    ring_buf[i * 3 + 2] = (uint8_t)(255 * b); // B
                }
                set_ring(ring_buf);
                step++;
                break;
            }

            case RING_SPEAK: {
                // Алиса говорит: живой, динамичный двухволновой эквалайзер речи!
                // Переливается между сочным маджента-фиолетовым (R=230, G=20, B=255)
                // и ярким неоновым бирюзовым (R=25, G=200, B=255) с гармониками амплитуды.
                float t = (float)step * 0.16f;
                for (int i = 0; i < NUM_LEDS; i++) {
                    float angle = (float)i * (2.0f * 3.14159f / (float)NUM_LEDS);
                    float wave1 = sinf(t * 1.8f + angle * 2.0f);
                    float wave2 = sinf(t * 2.7f - angle);
                    float amp = 0.30f + 0.70f * fabsf(0.6f * wave1 + 0.4f * wave2);
                    if (amp > 1.0f) amp = 1.0f;

                    float color_mix = 0.5f * (1.0f + sinf(t * 0.9f + angle));
                    // В формате драйвера (G, R, B):
                    uint8_t g_val = (uint8_t)((20.0f + 180.0f * (1.0f - color_mix)) * amp);
                    uint8_t r_val = (uint8_t)((230.0f * color_mix + 25.0f * (1.0f - color_mix)) * amp);
                    uint8_t b_val = (uint8_t)(255.0f * amp);

                    ring_buf[i * 3 + 0] = g_val; // G
                    ring_buf[i * 3 + 1] = r_val; // R
                    ring_buf[i * 3 + 2] = b_val; // B
                }
                set_ring(ring_buf);
                step++;
                break;
            }

            case RING_MUSIC: {
                float phase = (float)step * 0.05f;
                for (int i = 0; i < NUM_LEDS; i++) {
                    float offset = (float)i * (2.0f * 3.14159f / (float)NUM_LEDS);
                    float b = 0.2f + 0.8f * (0.5f * (1.0f + sinf(phase + offset)));
                    ring_buf[i * 3 + 0] = (uint8_t)(15 * b);
                    ring_buf[i * 3 + 1] = (uint8_t)(107 * b);
                    ring_buf[i * 3 + 2] = (uint8_t)(255 * b);
                }
                set_ring(ring_buf);
                step++;
                break;
            }
        }

        usleep(40000);
    }

    clear_ring();
    if (g_ring_fd >= 0) close(g_ring_fd);
    if (fifo_fd >= 0) close(fifo_fd);
    unlink(FIFO_PATH);
    return 0;
}
