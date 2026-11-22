#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <math.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <vector>

#include "tensorflow/lite/micro/micro_interpreter.h"
#include "tensorflow/lite/micro/micro_mutable_op_resolver.h"
#include "tensorflow/lite/micro/system_setup.h"
#include "tensorflow/lite/micro/schema/schema_generated.h"

#define SAMPLE_RATE 16000
#define FRAME_SIZE 400   // 25ms
#define FRAME_SHIFT 160  // 10ms
#define FFT_SIZE 512
#define NUM_MEL_BINS 40
#define BATCH_FRAMES 6   // 60ms batch
#include <glob.h>
#include <string>

#define RING_FIFO "/tmp/ring.fifo"
#define TRIGGER_FILE "/tmp/wakeword.trigger"
#define ALICE_BUSY_FILE "/tmp/alice_busy"
#define ACTIVATION_THRESHOLD 0.460f // Тишина <0.15, слабая «Алиса» ~0.45; ложные режутся 3 уверенными фреймами
#define PREAMP_GAIN 7.0f            // Matches Capsule Mini Speex mic level to model expectation

static std::string get_parec_cmd() {
    glob_t glob_res;
    if (glob("/tmp/pulse-*/native", 0, NULL, &glob_res) == 0 && glob_res.gl_pathc > 0) {
        std::string sock = glob_res.gl_pathv[0];
        globfree(&glob_res);
        return "PULSE_SERVER=unix:" + sock + " parec --device=speex --rate=16000 --channels=1 --format=s16le --latency-msec=20";
    }
    return "parec --device=speex --rate=16000 --channels=1 --format=s16le --latency-msec=20";
}

static volatile sig_atomic_t g_running = 1;

void sig_handler(int sig) {
    (void)sig;
    g_running = 0;
}

constexpr int kTensorArenaSize = 1024 * 1024;
static uint8_t tensor_arena[kTensorArenaSize];

// Mel filterbank weights
static float mel_filters[NUM_MEL_BINS][FFT_SIZE / 2 + 1];
static float hamming_window[FRAME_SIZE];

static float cos_table[FFT_SIZE / 2];
static float sin_table[FFT_SIZE / 2];
static int bit_reverse[FFT_SIZE];

void init_fft() {
    for (int i = 0; i < FFT_SIZE / 2; i++) {
        cos_table[i] = cosf(2.0f * M_PI * i / FFT_SIZE);
        sin_table[i] = sinf(2.0f * M_PI * i / FFT_SIZE);
    }
    for (int i = 0; i < FFT_SIZE; i++) {
        int rev = 0;
        for (int b = 0; b < 9; b++) { // log2(512) = 9
            if (i & (1 << b)) rev |= (1 << (8 - b));
        }
        bit_reverse[i] = rev;
    }
}

// In-place Cooley-Tukey Radix-2 FFT (0.04ms per frame, 305x faster than naive DFT)
void compute_fft(float *real, float *imag) {
    for (int i = 0; i < FFT_SIZE; i++) {
        int j = bit_reverse[i];
        if (i < j) {
            float tr = real[i]; real[i] = real[j]; real[j] = tr;
            float ti = imag[i]; imag[i] = imag[j]; imag[j] = ti;
        }
    }

    for (int len = 2; len <= FFT_SIZE; len <<= 1) {
        int half = len >> 1;
        int step = FFT_SIZE / len;
        for (int i = 0; i < FFT_SIZE; i += len) {
            for (int k = 0; k < half; k++) {
                int table_idx = k * step;
                float c = cos_table[table_idx];
                float s = sin_table[table_idx];
                float u_r = real[i + k];
                float u_i = imag[i + k];
                float v_r = real[i + k + half] * c + imag[i + k + half] * s;
                float v_i = imag[i + k + half] * c - real[i + k + half] * s;
                real[i + k] = u_r + v_r;
                imag[i + k] = u_i + v_i;
                real[i + k + half] = u_r - v_r;
                imag[i + k + half] = u_i - v_i;
            }
        }
    }
}

static float hz_to_mel(float hz) {
    return 1127.0f * logf(1.0f + hz / 700.0f);
}

static float mel_to_hz(float mel) {
    return 700.0f * (expf(mel / 1127.0f) - 1.0f);
}

void init_mel_filterbank() {
    for (int i = 0; i < FRAME_SIZE; i++) {
        hamming_window[i] = 0.54f - 0.46f * cosf(2.0f * M_PI * i / (FRAME_SIZE - 1));
    }

    float mel_min = hz_to_mel(20.0f);
    float mel_max = hz_to_mel(SAMPLE_RATE / 2.0f);
    float mel_step = (mel_max - mel_min) / (NUM_MEL_BINS + 1);

    float mel_points[NUM_MEL_BINS + 2];
    for (int i = 0; i < NUM_MEL_BINS + 2; i++) {
        mel_points[i] = mel_min + i * mel_step;
    }

    int bin_points[NUM_MEL_BINS + 2];
    for (int i = 0; i < NUM_MEL_BINS + 2; i++) {
        float hz = mel_to_hz(mel_points[i]);
        bin_points[i] = (int)floorf((FFT_SIZE + 1) * hz / SAMPLE_RATE);
        if (bin_points[i] > FFT_SIZE / 2) bin_points[i] = FFT_SIZE / 2;
    }

    memset(mel_filters, 0, sizeof(mel_filters));
    for (int m = 0; m < NUM_MEL_BINS; m++) {
        int left = bin_points[m];
        int center = bin_points[m + 1];
        int right = bin_points[m + 2];

        for (int k = left; k < center; k++) {
            if (center != left) {
                mel_filters[m][k] = (float)(k - left) / (center - left);
            }
        }
        for (int k = center; k <= right; k++) {
            if (right != center) {
                mel_filters[m][k] = (float)(right - k) / (right - center);
            }
        }
    }
}

void compute_log_mel(const int16_t *samples, float *out_mel) {
    float real[FFT_SIZE];
    float imag[FFT_SIZE];
    float prev = 0.0f;
    for (int i = 0; i < FRAME_SIZE; i++) {
        // Digital preamp gain 7.0x with saturation for Capsule Mini mic stream
        float cur = (float)samples[i] * PREAMP_GAIN;
        if (cur > 32767.0f) cur = 32767.0f;
        else if (cur < -32768.0f) cur = -32768.0f;

        // Pre-emphasis 0.72 from Yandex flags.txt (freq-filter-preemph-coeff 0.72)
        float val = i == 0 ? cur - 0.72f * cur : cur - 0.72f * prev;
        prev = cur;
        real[i] = val * hamming_window[i];
        imag[i] = 0.0f;
    }
    for (int i = FRAME_SIZE; i < FFT_SIZE; i++) {
        real[i] = 0.0f;
        imag[i] = 0.0f;
    }

    // High-speed Cooley-Tukey Radix-2 FFT
    compute_fft(real, imag);

    float power_spectrum[FFT_SIZE / 2 + 1];
    for (int k = 0; k <= FFT_SIZE / 2; k++) {
        power_spectrum[k] = real[k] * real[k] + imag[k] * imag[k];
    }

    for (int m = 0; m < NUM_MEL_BINS; m++) {
        float energy = 0.0f;
        for (int k = 0; k <= FFT_SIZE / 2; k++) {
            energy += power_spectrum[k] * mel_filters[m][k];
        }
        if (energy < 1e-10f) energy = 1e-10f;
        out_mel[m] = logf(energy);
    }
}

void trigger_wake() {
    // 1. Instantly light purple breathing ring
    int ring_fd = open(RING_FIFO, O_WRONLY | O_NONBLOCK);
    if (ring_fd >= 0) {
        write(ring_fd, "listen\n", 7);
        close(ring_fd);
    }
    // 2. Append timestamp to trigger file for capsula-alice
    FILE *f = fopen(TRIGGER_FILE, "a");
    if (f) {
        fprintf(f, "%ld\n", (long)time(NULL));
        fclose(f);
    }
}

int main(int argc, char **argv) {
    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);
    signal(SIGPIPE, SIG_IGN);

    tflite::InitializeTarget();
    init_fft();
    init_mel_filterbank();

    const char *model_path = argc > 1 ? argv[1] : "/data/model.tflite";
    if (access(model_path, R_OK) != 0) {
        if (access("/usr/share/voice-agent/model.tflite", R_OK) == 0) {
            model_path = "/usr/share/voice-agent/model.tflite";
        } else if (access("/tmp/model.tflite", R_OK) == 0) {
            model_path = "/tmp/model.tflite";
        }
    }

    printf("[wakewordd] Запуск оффлайн-детектора Алисы (TFLite Micro)...\n");
    printf("[wakewordd] Загрузка модели из: %s\n", model_path);
    printf("[wakewordd] Порог: %.3f, подтверждений: 3, cooldown: 2 c\n", ACTIVATION_THRESHOLD);
    fflush(stdout);

    FILE *mf = fopen(model_path, "rb");
    if (!mf) {
        fprintf(stderr, "[wakewordd] Ошибка открытия модели %s: %m\n", model_path);
        return 1;
    }
    fseek(mf, 0, SEEK_END);
    long msz = ftell(mf);
    fseek(mf, 0, SEEK_SET);
    std::vector<uint8_t> model_data(msz);
    fread(model_data.data(), 1, msz, mf);
    fclose(mf);

    const tflite::Model *model = tflite::GetModel(model_data.data());

    tflite::MicroMutableOpResolver<11> resolver;
    resolver.AddConcatenation();
    resolver.AddConv2D();
    resolver.AddDepthwiseConv2D();
    resolver.AddDequantize();
    resolver.AddHardSwish();
    resolver.AddMul();
    resolver.AddQuantize();
    resolver.AddSoftmax();
    resolver.AddStridedSlice();
    resolver.AddSum();
    resolver.AddTranspose();

    tflite::MicroInterpreter interpreter(model, resolver, tensor_arena, kTensorArenaSize);
    if (interpreter.AllocateTensors() != kTfLiteOk) {
        fprintf(stderr, "[wakewordd] Ошибка выделения тензоров в TFLM!\n");
        return 1;
    }
    printf("[wakewordd] Тензоры успешно аллоцированы в RAM (%zu байт).\n", interpreter.arena_used_bytes());

    // Zero out states
    for (int s = 1; s <= 9; s++) {
        TfLiteTensor *in_state = interpreter.input(s);
        memset(in_state->data.int8, 0, in_state->bytes);
    }

    TfLiteTensor *input_frames = interpreter.input(0); // [1, 1, 6, 80]
    float *frames_ptr = input_frames->data.f;

    float mel_batch[BATCH_FRAMES][NUM_MEL_BINS];
    int batch_idx = 0;

    int16_t sample_window[FRAME_SIZE];
    memset(sample_window, 0, sizeof(sample_window));

    time_t last_trigger_time = 0;

    while (g_running) {
        printf("[wakewordd] Подключение к микрофону parec (speex 16kHz mono)...\n");
        fflush(stdout);

        std::string cmd = get_parec_cmd();
        printf("[wakewordd] Команда захвата: %s\n", cmd.c_str());
        fflush(stdout);

        FILE *pipe = popen(cmd.c_str(), "r");
        if (!pipe) {
            perror("[wakewordd] popen parec failed");
            sleep(2);
            continue;
        }
        setvbuf(pipe, NULL, _IONBF, 0);

        int16_t hop_buffer[FRAME_SHIFT]; // 160 samples (10ms)
        while (g_running) {
            size_t n = fread(hop_buffer, sizeof(int16_t), FRAME_SHIFT, pipe);
            if (n < FRAME_SHIFT) {
                fprintf(stderr, "[wakewordd] Поток микрофона прервался.\n");
                break;
            }

            // Shift sample window left by 160 and append new 160 samples
            memmove(sample_window, sample_window + FRAME_SHIFT, (FRAME_SIZE - FRAME_SHIFT) * sizeof(int16_t));
            memcpy(sample_window + (FRAME_SIZE - FRAME_SHIFT), hop_buffer, FRAME_SHIFT * sizeof(int16_t));

            // Compute 40 log-mel features for this 10ms step
            compute_log_mel(sample_window, mel_batch[batch_idx]);
            batch_idx++;

            // Check if Alice daemon is busy (recording command, thinking, answering)
            if (access(ALICE_BUSY_FILE, F_OK) == 0) {
                for (int s = 1; s <= 9; s++) {
                    TfLiteTensor *in_state = interpreter.input(s);
                    memset(in_state->data.int8, 0, in_state->bytes);
                }
                memset(sample_window, 0, sizeof(sample_window));
                batch_idx = 0;
                last_trigger_time = time(NULL);
                continue;
            }

            // Every 6 frames (60ms), run inference
            if (batch_idx == BATCH_FRAMES) {
                for (int f = 0; f < BATCH_FRAMES; f++) {
                    for (int b = 0; b < NUM_MEL_BINS; b++) {
                        frames_ptr[f * 80 + b] = mel_batch[f][b];
                        frames_ptr[f * 80 + NUM_MEL_BINS + b] = mel_batch[f][b];
                    }
                }

                if (interpreter.Invoke() == kTfLiteOk) {
                    // Update state tensors
                    for (int s = 1; s <= 9; s++) {
                        TfLiteTensor *out_state = interpreter.output(s);
                        TfLiteTensor *in_state = interpreter.input(s);
                        memcpy(in_state->data.int8, out_state->data.int8, out_state->bytes);
                    }

                    // Read logits: [1, 1, 6, 2]
                    TfLiteTensor *logits = interpreter.output(0);
                    float *l_data = logits->data.f;
                    float peak_p = 0.0f;

                    int sustained_frames = 0;
                    for (int step = 0; step < BATCH_FRAMES; step++) {
                        float f_logit = l_data[step * 2 + 0];
                        float a_logit = l_data[step * 2 + 1];
                        float max_l = f_logit > a_logit ? f_logit : a_logit;
                        float p_alisa = expf(a_logit - max_l) / (expf(f_logit - max_l) + expf(a_logit - max_l));
                        if (p_alisa > peak_p) peak_p = p_alisa;
                        if (p_alisa >= 0.42f) sustained_frames++;
                    }

                    time_t now = time(NULL);
                    static time_t last_log = 0;
                    static float window_max_score = 0.0f;
                    if (peak_p > window_max_score) window_max_score = peak_p;
                    if (now - last_log >= 5) {
                        last_log = now;
                        printf("[wakewordd] Слушаю микрофон... (пик за 5с: %.4f)\n", window_max_score);
                        fflush(stdout);
                        window_max_score = 0.0f;
                    }

                    if (peak_p >= ACTIVATION_THRESHOLD && sustained_frames >= 3 && (now - last_trigger_time >= 2)) {
                        last_trigger_time = now;
                        printf("\n[wakewordd] >>> АЛИСА ОБНАРУЖЕНА! (score: %.3f) <<<\n", peak_p);
                        fflush(stdout);

                        trigger_wake();

                        // Reset states and sample window after wake
                        for (int s = 1; s <= 9; s++) {
                            TfLiteTensor *in_state = interpreter.input(s);
                            memset(in_state->data.int8, 0, in_state->bytes);
                        }
                        memset(sample_window, 0, sizeof(sample_window));
                    }
                }

                batch_idx = 0;
            }
        }

        pclose(pipe);
        if (g_running) sleep(1);
    }

    printf("[wakewordd] Завершение работы детектора.\n");
    return 0;
}
