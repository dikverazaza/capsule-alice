# Capsule Alice на VK Capsula Mini (A113X) — рабочий гайд

Это не «теоретическая» инструкция, а конспект реальной установки, которая завелась на колонке. Стог голосового ассистента Маруся не перепрошивается — он только отключается init-stub'ами. Root не нужен.

## 1. Что должно быть на колонке

- Стоковый софт после factory reset, Wi‑Fi подключён.
- TCP ADB `:5555` включён. USB-ADB в этой сборке **отсутствует**.
- Активный слот `_b`; патч ADB лежит в `system_b`:
  - `usr/bin/adbd-tcp`
  - `etc/init.d/S91adbd.sh`
- Если ADB нет, сначала прошить `system_b` через Tools Lite (`core/write_lite.py --part system_b ...`) с `wpa_supplicant.conf` и `adbd-tcp`.

Проверка с PC:

```bash
python -c "import socket;s=socket.socket();s.settimeout(2);print(s.connect_ex(('192.168.1.196',5555)))"
```

`0` — порт открыт.

## 2. Инструменты на PC

- `cargo` + `cargo-zigbuild`
- `zig`
- Python 3

В Git Bash обязательно отключать преобразование путей, иначе `/usr/bin/...` превратится в `C:/Program Files/...`:

```bash
export MSYS_NO_PATHCONV=1
```

В PowerShell этой проблемы нет.

### 2.1. ADB-клиент без `adb.exe`

Использованы два скрипта из `_diag/`:

- `adb_shell.py` — TCP ADB shell (CNXN/OPEN/OKAY/CLSE).
- `adb_push.py` — загрузка файлов через `shell:cat > path`.

Важно: данные в `shell:cat` идут **не** `OKAY`-пакетами, а `WRTE` с stream ID из ответа `OPEN`, после каждого `WRTE` — пустой `OKAY` для flow control. Иначе файлы получаются нулевого размера.

Пример:

```bash
MSYS_NO_PATHCONV=1 python _diag/adb_shell.py --ip 192.168.1.196 --cmd 'id; cat /etc/hostname'
MSYS_NO_PATHCONV=1 python _diag/adb_push.py --local FILE --remote /usr/bin/FILE --mode 755
```

## 3. Сборка

### 3.1. Подготовка исходников

```bash
git clone --depth 1 https://github.com/dikverazaza/capsule-alice.git capsule-alice-upstream
cd capsule-alice-upstream
mkdir -p web build-out
```

Required правки, без которых сборка падает:

1. Создать `web/index.html` — `include_str!("../web/index.html")` иначе ошибка `couldn't read src\../web/index.html`.
2. В `src/bin/capsula-yandex.rs` добавить поле `disabled` в `AuthData`:

```rust
auth: AuthData {
    salt: String::new(),
    hash: String::new(),
    disabled: false,
},
```

3. Для wakeword использовать TFLM flatbuffers `25.9.23`, Release/O3/SPEED и закомментировать проверку в `softmax_common.cc`:

```cpp
// TF_LITE_ENSURE(context, output->params.scale == 1.f / 256);
```

### 3.2. Rust

```bash
export PATH="/c/Users/coco/.cargo/bin:$PATH"
cargo zigbuild --release --target aarch64-unknown-linux-musl
cp target/aarch64-unknown-linux-musl/release/capsula-alice build-out/
cp target/aarch64-unknown-linux-musl/release/capsula-yandex build-out/
cp target/aarch64-unknown-linux-musl/release/capsula-audio build-out/
cp target/aarch64-unknown-linux-musl/release/capsula-hub build-out/
```

### 3.3. C/C++ сервисы

```bash
zig cc -target aarch64-linux-musl -O2 \
  services/ring_daemon/ring_daemon.c -o build-out/ring_daemon -lpthread

zig cc -target aarch64-linux-musl -O2 \
  services/onboarding/onboarding_daemon.c -o build-out/capsule_onboarding -lpthread

zig c++ -target aarch64-linux-musl -O2 -std=c++17 \
  -DFLATBUFFERS_LOCALE_INDEPENDENT=0 \
  -I"C:/Users/coco/toolchains/tflite-micro" \
  -I"C:/Users/coco/toolchains/flatbuffers-25.9.23/include" \
  services/wakeword/wakewordd.cc \
  "C:/Users/coco/toolchains/tflite-micro/build-rel/libtflm_micro.a" \
  -o build-out/wakewordd
```

`-DFLATBUFFERS_LOCALE_INDEPENDENT=0` нужен, иначе musl ругается на `strtoll_l`/`strtoull_l`.

## 4. Установка на колонку

### 4.1. Что заливать

```text
/usr/bin/capsula-alice
/usr/bin/capsula-yandex
/usr/bin/capsula-audio
/usr/bin/capsula-hub
/usr/bin/wakewordd
/usr/bin/ring_daemon
/usr/bin/capsule_onboarding
/usr/bin/capsula-alice-launcher.sh
/usr/bin/wifi_setup_helper.sh
/usr/bin/capsule-yandex-account
/usr/bin/capsule-mic
/data/model.tflite
/etc/init.d/S99capsula-alice
/etc/init.d/S99capsula-alice-autostart.sh
/etc/init.d/S50avahi-daemon
/etc/init.d/S97voice-agent.sh
/etc/init.d/S97okcall.sh
/etc/avahi/services/http.service
/etc/hostname
```

Перед заменой бинарника обязательно убить процесс. Иначе `cat > /usr/bin/...` может молча не перезаписать запущенный файл (`ETXTBSY`), а размер останется старым.

### 4.2. Отключение Маруси

Не переименовываем стоковые файлы — это создаёт whiteout-проблемы. Делаем безопасные stub’ы:

```sh
#!/bin/sh
exit 0
```

Их нужно положить в:

```text
/etc/init.d/S97voice-agent.sh
/etc/init.d/S97okcall.sh
```

### 4.3. Launcher

`/usr/bin/capsula-alice-launcher.sh`:

- создаёт `/run/capsula-yandex`, `/data/capsula-yandex`, FIFOs;
- ждёт PulseAudio socket `/tmp/pulse-*/native`;
- выставляет `PULSE_SERVER`;
- запускает `ring_daemon`, `capsule_onboarding`, `wifi_setup_helper.sh`, `capsula-yandex`, `wakewordd`, `capsula-alice`.

Сервисы запускаются через `nohup` **внутри launcher’а** — тогда они выживают после закрытия ADB-сессии. Прямой `nohup ... &` из ADB-шела на этой прошивке не работает.

### 4.4. Автозапуск

На этой сборке `rcS` не долетает до `S99`-скриптов. Рабочий триггер — `S50avahi-daemon`, он реально выполняется.

`/etc/init.d/S50avahi-daemon` при `start`:

```sh
hostname alice 2>/dev/null
echo alice > /etc/hostname 2>/dev/null
killall -9 avahi-daemon 2>/dev/null
$DAEMON --no-drop-root -D

if ! pgrep -a capsula-alice >/dev/null 2>&1; then
    nohup /usr/bin/capsula-alice-launcher.sh >/tmp/capsula-alice-launcher.log 2>&1 &
fi
```

Стоковый `S23hostname.sh` всё равно перезаписывает hostname при загрузке, поэтому `hostname alice` выставляется и в `S50`, и в launcher’е.

### 4.5. Стоп-скрипт — важная ловушка

В `stop_all` **нельзя** писать:

```sh
echo "off" > /tmp/ring.fifo
```

Если `ring_daemon` уже убит, это блокирует `rcK` в `pipe_wait` и колонка висит при ребуте. Строку нужно удалить.

## 5. Привязка Яндекс-аккаунта

### 5.1. Причина сбоя веб-портала

Оригинальный `capsule_onboarding` дергал:

```text
/usr/lib/capsula-root/bin/curl
```

После factory reset этого файла нет. `/api/yandex_code` возвращал пустой ответ, поэтому привязка через веб-UI не работала.

Исправление — заменить все три вызова на BusyBox `wget`:

```sh
wget -q -O - -T 15 --post-data 'client_id=...' https://oauth.yandex.ru/device/code
wget -q -O - -T 15 --post-data 'grant_type=device_code&code=...' https://oauth.yandex.ru/token
wget -q -O - -T 15 --header 'Authorization: OAuth ...' 'https://login.yandex.ru/info?format=json'
```

Пересобрать `capsule_onboarding` и залить на устройство.

### 5.2. Проверка API портала

```bash
wget -qO- http://127.0.0.1/api/status
wget -qO- http://127.0.0.1/api/yandex_code
```

Должен вернуться JSON с `user_code`, `device_code`, `verification_url`.

### 5.3. Привязка

Открыть:

```text
http://192.168.1.196/
```

или:

```text
http://alice.local/
```

Портал покажет код и ссылку `https://ya.ru/device`. После подтверждения токены пишутся в:

```text
/etc/alice/yandex_credentials.json
/data/capsula-yandex/credentials.json
/root/.capsula-mini/yandex_credentials.json
```

Проверка:

```sh
capsule-yandex-account status
```

Резервный вариант — `_diag/yandex_device_login.sh`, запускать через `start-stop-daemon -S -b`, не через `nohup`.

## 6. Проверка после установки

```sh
/etc/init.d/S99capsula-alice status
netstat -lntp | grep -E ':80 |:8080 |:5555 '
hostname
capsule-yandex-account status
tail -f /tmp/capsula-alice.log
tail -f /tmp/wakewordd.log
```

Ожидаемые процессы:

```text
ring_daemon
capsule_onboarding
wifi_setup_helper.sh
capsula-yandex
wakewordd
capsula-alice
```

Wakeword в логе:

```text
[wakewordd] >>> АЛИСА ОБНАРУЖЕНА! (score: ...)
```

`/api/health` в этой сборке отсутствует — это норма. Портал отвечает на `/`.

## 7. Известные ошибки и их причины

| Симптом | Причина | Лечение |
|---|---|---|
| Файлы залились 0 байт | `OKAY` вместо `WRTE` в ADB push | Исправить `adb_push.py` |
| `/usr/bin/...` превращается в `C:/Program Files/...` | Git Bash path conversion | `MSYS_NO_PATHCONV=1` |
| Сервисы умирают после закрытия ADB | `nohup ... &` из ADB-шела | Запускать launcher через `S50avahi-daemon`; внутри launcher — `nohup` |
| Ребут висит в `rcK` | `echo off > /tmp/ring.fifo` без читателя | Удалить строку |
| Портал не выдаёт код | Нет `/usr/lib/capsula-root/bin/curl` | Переписать на `wget` |
| S99 не стартует на boot | `rcS` не доходит до S99 на этой прошивке | Триггер в `S50avahi-daemon` |
| `sh: awk: not found` | BusyBox собран без applet `awk` | Не использовать awk; разбирать `pactl` shell-циклом |

## 8. Задержка ответа и wake word

Рабочие замеры:

```text
[Latency] ASR: 449 ms
[Latency] TTS first chunk: 514 ms (Vins+WS total: 900 ms)
```

Важно: агрессивное уменьшение VAD до 12 чанков (240 мс) **сломало распознавание** — ASR стал возвращать пустоту за 3.8–4.3 с. Поэтому VAD возвращён к надёжным значениям:

- пауза: `28 чанков` (560 мс);
- старт речи: `3 чанка` (60 мс).

Что реально ускоряет ответ без потери качества:

- Два WebSocket греются **параллельно во время записи команды**: один для ASR, второй для Vins. Раньше Vins открывал соединение уже после ASR, и холодный TLS-handshake давал задержку ~20 с (`уже проиграно 21.14 с` в логе). `execute_command()` теперь получает готовый `vins_ws`.
- Тот же прогретый канал используется для локальных ответов («Слушаю вас»): `speak_text_with_ws()` → `synthesize_tts_pcm(text, tts_ws)`. Раньше локальный ответ снова делал холодный `connect_and_sync()`.
- Кольцо больше не показывает `speak` во время синтеза: `set_ring("think")` до TTS, `set_ring("speak")` только перед реальным воспроизведением PCM.
- TTS `quality`: `UltraHigh` → `Normal`;
- `pacat --latency-msec`: 120 → 80;
- TTS-поток обрывается через `1200 мс` после последнего аудиочанка, а не ждёт 9–15 с `streamcontrol`. Это убирает «длинный хвост», когда Алиса уже договорила, но всё ещё считается занятой.
- Если TTS-запрос ушёл, но за `4500 мс` не пришёл ни один аудиочанок, запрос прерывается и Алиса говорит fallback-фразу. Раньше в этом случае main loop висел до 15 с с открытым `pacat`.
- Ожидание пустого ASR сокращено с `3500 мс` до `2200 мс`: пустые распознавания больше не висят 4.2 с.
- В окне продолжения диалога (`4 с`) Алиса реагирует на wake word/касание **или** на уверенную речь после `700 мс` защиты от эха. Порог RMS: `max(noise*1.2 + 50, 260)`, нужно `6 чанков` подряд.
- Основной порог речи снижен: `max(noise*1.10 + 30, 150)` вместо `max(noise*1.5 + 80, 280)`. Старт фразы — `3 чанка` (60 мс).
- Шумовой пол жёстко ограничен `clamp(80, 220)`: раньше речь поднимала его до 500, порог догонял речь (580) и колонка «не слышала». Теперь при фоне ~207 порог ~258, а речь даёт 400–500.
- При зависании помогает очистка клиентов PulseAudio: `killall -9 pactl pacat parec`, затем перезапуск launcher’а.
- Fire-and-forget-процессы (`pactl`, `sh`, `paplay`) теперь идут через `spawn_reaped()`: отдельный поток сразу делает `wait()`, поэтому зомби не копятся. Проверка: `for p in /proc/[0-9]*; do [ "$(cut -d" " -f3 "$p/stat")" = Z ] && echo "$p"; done`.
- После замены `/usr/bin/capsula-alice-launcher.sh` не запускать его сразу в той же сессии: возможен кратковременный `Text file busy`. Подождать секунду и запустить отдельной командой.
- Микрофон `speex` выставлен в `150%` (10.57 dB) в launcher’е и в стартовом скрипте `capsula-alice`. При 100% приходилось кричать, при 250% шумовой пол насыщался до 500 RMS и порог речи уезжал к 665 — Алиса переставала слышать. 150% + порог `max(noise*1.10 + 30, 150)` дают рабочий фон ~140 и порог ~180.

`FOLLOWUP_WINDOW = 4 с` — окно продолжения диалога без повторного wake word. Оно **не менялось**. Если Алиса «долго обрабатывает», смотреть нужно на `ASR` и `TTS first chunk`, а не на `FOLLOWUP_WINDOW`.

### Фраза «Алиса привет» одним дыханием

Раньше при активации код делал `while pcm_rx.try_recv().is_ok() {}` и терял уже пришедшие чанки с «привет». Исправлено:

- `audio_pre_buffer` копируется в `pending_seed` и передаётся в `pre_chunks`;
- `pcm_rx` при активации **не очищается**;
- буфер увеличен с `10` до `25 чанков` (500 мс), чтобы «привет» не обрезался поначалу;
- если `pre_chunks` уже содержат уверенную речь (≥3 чанков выше порога), запись считается начатой **сразу**, без ожидания нового всплеска RMS. Иначе «привет», сказанный сразу после «Алиса», сбрасывался;
- короткие фразы `привет`, `здравствуй`, `как дела` и т.п. отвечаются локально через TTS, без Vins/YandexGPT.

### Wake word

Порог в `wakewordd.cc`:

```cpp
#define ACTIVATION_THRESHOLD 0.460f
```

Подтверждение — 3 уверенных фрейма, cooldown 2 с. Порог выведен из замеров на этой колонке: тишина `0.10–0.15`, тихая речь даёт пики `0.35–0.45`, произнесённая «Алиса» в разных попытках давала `0.5968` и ниже. `0.650` и `0.580` пропускали фразы. Текущее значение `0.460` + 3 подтверждения. В лог старта добавлена строка `[wakewordd] Порог: 0.460, подтверждений: 3, cooldown: 2 c` — по ней сразу видно, какая прошивка детектора активна. При ложных срабатываниях поднять до `0.500`, но не возвращать `0.650`.

Все `awk` заменены на shell-циклы: BusyBox на этой колонке собран без applet `awk`.

## 9. Текущие параметры установки

- IP: `192.168.1.196`
- Имя: `alice` / `alice.local`
- Wi-Fi: `Wi-Fi`, пароль `CILAEV01`
- Прошивка: сток `44.58-hf-4-kws-el-2`
- Root: не установлен после factory reset, для Alice не требуется
