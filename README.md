# Capsule Alice

Автономный стек голосового ассистента Алиса и стриминга Яндекс Музыки для умной колонки VK Капсула Мини (платформа Amlogic A113X). Реализован на Rust и C/C++ поверх штатного Buildroot, PulseAudio и ALSA — без контейнеров, внешних рантаймов и телеметрии.

Основная колонка (`alice`) использует ADB-over-TCP и локальный offline-споттер. Вторая колонка сохраняет заводской hostname, но получает тот же OTA-клиент и тот же набор служб.

---

### Аппаратно-ресурсная матрица

| Компонент | Железо / Интерфейс | Служба | RSS, факт | Роль в системе |
| :--- | :--- | :--- | :--- | :--- |
| **Речевой интерфейс** | Yandex Uniproxy (WebSocket) | `capsula-alice` (Rust) | ~2.1 МБ | ASR, TTS, Vins-диалог, сценарии, clock-оверлей |
| **Аудиоплеер** | PulseAudio sink / HTTP stream | `capsula-yandex` (Rust) | ~2.9 МБ | «Моя волна», треклисты, потоковое воспроизведение |
| **Радио-конвейер** | HTTP MP3 → raw PCM | `capsula-audio` + `paplay` | по требованию | Именованные радиостанции вне Yandex-очереди |
| **Детектор активации** | 16 kHz PCM / TFLite Micro | `wakewordd` (C++) | ~1.9 МБ | Локальный споттер «Алиса», порог 0.300 |
| **LED-кольцо** | AW9523B, I2C bus 1 | `ring_daemon` (C) | <1 МБ | 12-зонные состояния: listen / think / speak / mute |
| **Экран часов** | LED-матрица IS31FL3236 + BH1730 | встроено в `capsula-alice` | в RSS выше | Цифровое время, автояркость по ALS |
| **Микрофонный ключ** | Сенсорная кнопка mute | `capsule-mic` (sh) | — | Hardware mute capture + индикация кольца |
| **Портал настройки** | HTTP 80 / 8080, RFCOMM 3 | `capsule_onboarding` (C) | ~2 МБ | Wi-Fi, Яндекс ID, Bluetooth SPP Setup |
| **Автообновление** | GitHub raw / `main` | `capsula-ota` (POSIX sh) | ~1.5 МБ | SHA-256, atomic install, rollback, 600 с |

Фактический RSS ядра без PulseAudio: **~9–12 МБ** из ~240 МБ видимой RAM. В простое потребление стека вместе с PulseAudio обычно не превышает **20 МБ**.

---

### Схема взаимодействия (IPC)

Компоненты изолированы; обмен идёт через Unix FIFO и файлы-флаги в `/tmp/` и `/run/`:

```
                         +------------------------+
                         |   PulseAudio / ALSA    |
                         | speex source | master  |
                         +-------+------+---------+
                                 | 16 kHz PCM
                    +------------+------------+
                    |                         |
                    v                         v
          +-------------------+      +--------------------+
          |     wakewordd     |      |   capsula-alice    |
          | TFLM, /data/model |      | ASR/TTS/Vins/LED   |
          +---------+---------+      +----+-------+------+
                    | /tmp/wakeword.trigger    |       |
                    +------------------------->|       |
                                               |       | /tmp/ring.fifo
                                               v       v
                                      +-----------------------+
                                      |     ring_daemon       |
                                      | AW9523B, I2C bus 1    |
                                      +-----------------------+

  /run/capsula-yandex/control : capsula-alice <-> capsula-yandex
  /tmp/alice_busy             : флаг занятости для wakewordd/кольца
  /tmp/capsula-yandex/*       : состояние плеера и радио-конвейера
  WebSocket                   : capsula-alice <-> Yandex Uniproxy
```

`capsula-audio` декодирует HTTP-поток в raw `s16le`; `paplay` запускается с `--raw --format=s16le --rate=44100 --channels=2`. Без флага `--raw` радио падало с `Failed to open audio file`.

---

### Сборка из исходников

Кросс-компиляция выполняется под `aarch64-unknown-linux-musl`.

#### 1. Сервисы на Rust

```bash
rustup target add aarch64-unknown-linux-musl

cargo build --release --target aarch64-unknown-linux-musl \
  --bin capsula-alice --bin capsula-yandex --bin capsula-audio

# Вариант с Zig вместо rustup target:
cargo zigbuild --release --target aarch64-unknown-linux-musl \
  --bin capsula-alice --bin capsula-yandex --bin capsula-audio
```

#### 2. Низкоуровневые демоны (C / C++ через Zig)

```bash
make -C services/ring_daemon
make -C services/onboarding

# wakewordd.cc в репозитории — синхронизированный источник параметров.
# Готовый бинарник из ota/assets/wakewordd собран с TFLite Micro и
# бинарно пропатчен на порог 0.300; использовать его как артефакт.
zig c++ -target aarch64-linux-musl -O2 \
  services/wakeword/wakewordd.cc -o wakewordd -lm
```

---

### Установка на колонку

Рекомендуемый путь — первичная загрузка OTA-клиента и доставка всего остального через манифест. Прямое копирование бинарников допустимо только как аварийный метод.

Доступ:
*Primary*: `adb connect 192.168.1.196:5555`.
*Second*: SSH `root@192.168.1.113` (пароль задаётся на устройстве, в репозитории не хранится).
Обе колонки находятся в одной сети; ADB/SSH-сессия не должна использоваться как постоянный supervisor — службы запускаются через init.

#### 1. Bootstrap OTA-клиента

```bash
# Primary, через ADB
adb connect 192.168.1.196:5555
adb push ota/assets/capsula-ota.sh /data/local/tmp/capsula-ota
adb push ota/assets/S98capsula-ota /data/local/tmp/S98capsula-ota
adb shell chmod 755 /data/local/tmp/capsula-ota /data/local/tmp/S98capsula-ota
adb shell cp /data/local/tmp/capsula-ota /usr/bin/capsula-ota
adb shell cp /data/local/tmp/S98capsula-ota /etc/init.d/S98capsula-ota
adb shell chmod 755 /usr/bin/capsula-ota /etc/init.d/S98capsula-ota
adb shell /usr/bin/capsula-ota check
adb shell /usr/bin/capsula-ota status

# Second, через SSH
scp ota/assets/capsula-ota.sh root@192.168.1.113:/usr/bin/capsula-ota
scp ota/assets/S98capsula-ota root@192.168.1.113:/etc/init.d/S98capsula-ota
ssh root@192.168.1.113 'chmod 755 /usr/bin/capsula-ota /etc/init.d/S98capsula-ota; /usr/bin/capsula-ota check; /usr/bin/capsula-ota status'
```

На Windows/MSYS2 прямой `adb push ... /usr/bin/...` может быть испорчен конвертером путей. Заливайте в `/data/local/tmp/` и делайте `mv` внутри `adb shell`.

Первый `check` может увидеть ранее закэшированный raw-манифест. Это безопасно: версионная блокировка запрещает даунгрейд, а следующий check через 10 минут подтянет свежую сборку. Ускоренно: повторите `capsula-ota check` вручную.

#### 2. Полная ручная установка (только при недоступном OTA)

```bash
# Исполняемые файлы стека — брать из ota/assets/ или собрать самостоятельно
cp ota/assets/capsula-alice ota/assets/capsula-yandex ota/assets/capsula-audio \
   ota/assets/wakewordd ota/assets/ring_daemon ota/assets/capsula-alice-launcher.sh \
   /usr/bin/
chmod 755 /usr/bin/capsula-alice /usr/bin/capsula-yandex /usr/bin/capsula-audio \
          /usr/bin/wakewordd /usr/bin/ring_daemon /usr/bin/capsula-alice-launcher.sh

# Веса offline-споттера
cp models/model.tflite /data/model.tflite
chmod 644 /data/model.tflite

# Вспомогательные сценарии
cp scripts/wifi_setup_helper.sh scripts/capsule-yandex-account scripts/capsule-mic /usr/bin/
chmod 755 /usr/bin/wifi_setup_helper.sh /usr/bin/capsule-yandex-account /usr/bin/capsule-mic

# Init: S50 — boot-hook, S98 — standalone OTA daemon
cp scripts/S50avahi-daemon /etc/init.d/S50avahi-daemon
cp ota/S98capsula-ota /etc/init.d/S98capsula-ota
chmod 755 /etc/init.d/S50avahi-daemon /etc/init.d/S98capsula-ota

# Конфиг OTA и identity flag
cp ota/assets/capsula-ota.conf /etc/capsula-ota.conf
chmod 644 /etc/capsula-ota.conf
# ONLY primary capsule:
printf 'alice\n' > /etc/alice-identity
# Second capsule: НЕ создавать /etc/alice-identity — сохраняем factory hostname.
```

`install/S99capsula-alice`, `install/S97voice-agent.sh`, `install/S97okcall.sh` оставлены для совместимости. На второй колонке **не перезаписывайте** `S97voice-agent.sh` пустышкой: штатный скрипт там является резервным триггером launcher. На primary он уже заменён заглушкой.

Перенос LF выполняется на хосте (`dos2unix`), не на колонке: BusyBox на A113X не содержит `sed -i`.

```bash
# Первый запуск
/etc/init.d/S50avahi-daemon start
/etc/init.d/S98capsula-ota start

# Проверка
/usr/bin/capsula-ota status
ps | grep -E 'capsula-alice|wakewordd|ring_daemon|capsula-ota' | grep -v grep
```

`S50avahi-daemon` поднимает avahi, Bluetooth SPP setup (если присутствует), Alice launcher и OTA daemon. `S98capsula-ota` — независимый дублирующий старт OTA; `/data/ota/.daemon.lock` исключает два демона. Launcher имеет собственный lock `/var/run/capsula-alice-launcher.lock`, поэтому S50 и S97 не создают два супервизора.

Launcher **никогда не поднимает громкость**: только ограничивает её до 20%, если она выше. Микрофон `speex` выставляется в 150%, mute сохраняется.

---

### Конфигурация и авторизация

1. **Wi-Fi.**
   * Primary: HTTP-портал на 80/8080, fallback AP `Setup`, адрес `http://alice.local`.
   * Second: тот же портал плюс Bluetooth SPP `Capsule-Setup`, RFCOMM channel 3. Через него работает Android APK (Wi-Fi scan / Yandex device-code / STATUS / REBOOT).
2. **Яндекс ID.**
   * В веб-интерфейсе: «Получить код для входа» → [ya.ru/device](https://ya.ru/device).
   * Терминал колонки:
     ```bash
     capsule-yandex-account status
     capsule-yandex-account login
     capsule-yandex-account reset
     ```
   * Launcher копирует `/etc/alice/yandex_credentials.json` в `/data/capsula-yandex/credentials.json`, если файл отсутствует.
3. **Радио.**
   * Команда «Алиса, включи радио Ваня» использует проверенный MP3-поток и raw-конвейер `capsula-audio | paplay`.
   * «Алиса, выключи радио», «Алиса, стоп радио» и просто «Алиса, стоп» убивают конвейер `capsula-audio`/`paplay`.

---

### Обновления по воздуху (OTA)

Клиент работает без токена на устройстве. Манифест и ассеты берутся из публичных raw-файлов ветки `main`. Подвижный тег `ota-latest` используется только как архивная метка — raw CDN кэширует перемещённые теги слишком долго.

```bash
capsula-ota check        # проверка и установка новой версии
capsula-ota force        # принудительная переустановка
capsula-ota status       # version / daemon / lock / backup / last result
capsula-ota rollback     # откат на последний бэкап
```

Гарантии:

* SHA-256 + размер проверяются до замены любого файла.
* Стадирование в `/tmp`, установка через `<dest>.new` + atomic `mv`.
* Whitelist путей назначения; произвольная запись в ФС невозможна.
* Anti-downgrade через `sort -V`: stale raw-манифест не откатит колонку.
* Backup + rollback. Если `/data` почти полон — snapshot уходит в tmpfs.
* Демон независим от Alice/PulseAudio: проверка каждые 600 с, повтор через 60 с после ошибки, автозапуск при загрузке.
* Ночное окно 23:00–07:00 откладывает перезапуск служб, но не сами загрузки.

Локальный конфиг: `/etc/capsula-ota.conf`. После редактирования перезапустите демон:

```bash
/etc/init.d/S98capsula-ota restart
cat /etc/capsula-ota.conf
```

Подробная спецификация публикации: [`ota/README.md`](ota/README.md).

---

### Лицензия

MIT License. См. [`LICENSE`](LICENSE) для полных условий.
