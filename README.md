# Capsule Alice

Прошивка и программный стек для запуска голосового ассистента Алиса и Яндекс Музыки на умной колонке **VK Капсула Мини** (Amlogic A113X).

---

## 🌟 Основные возможности

* **Голосовой ассистент Алиса:** Полноценный клиент Yandex Uniproxy (распознавание речи ASR, синтез TTS, диалоговая логика Vins и сценарии умного дома).
* **Яндекс Музыка:** Автономный плеер потокового аудио («Моя волна», воспроизведение треков, плейлистов, лайки/дизлайки через FIFO интерфейс).
* **Оффлайн-споттер ключевого слова:** Демон `wakewordd` на базе TensorFlow Lite Micro с быстрым БПФ (Cooley-Tukey Radix-2) и MFCC-фильтрацией для надёжного распознавания слова «Алиса» без отправки постоянного аудио в сеть.
* **Светодиодное кольцо (AW9523B):** Анимации ожидания, прослушивания, мышления, ответа, ночник и плавный оверлей изменения громкости.
* **LED-экран часов с автояркостью:** Отображение времени на матричном дисплее с динамической регулировкой яркости по встроенному датчику освещённости BH1730.
* **Аппаратный мьют микрофона:** Включение и выключение микрофона сенсорной кнопкой на панели с индикацией красным кольцом и консольной утилитой `capsule-mic`.
* **Веб-портал онбординга:** Локальный веб-интерфейс на портах 80 и 8080 для выбора сети Wi-Fi и быстрой привязки Яндекс ID через [ya.ru/device](https://ya.ru/device).
* **Резервная сеть Setup:** Автоматическое подключение к хотспоту `Setup` в режиме первоочередного приоритета для простой настройки в незнакомой сети.

---

## 🏗 Архитектура системы

```
                     ┌────────────────────────┐
                     │     PulseAudio ALSA    │
                     └───────────┬────────────┘
                                 │ PCM 16kHz
                                 ▼
                     ┌────────────────────────┐
                     │       wakewordd        │◄── model.tflite
                     └───────────┬────────────┘
                                 │ trigger
                                 ▼
┌──────────────────┐       ┌───────────┐       ┌──────────────────┐
│  capsula-yandex  │◄─────►│capsula-   │◄─────►│   ring_daemon    │
│  (Яндекс Музыка) │ FIFO  │ alice     │ FIFO  │ (LED-кольцо AW95)│
└──────────────────┘       └─────┬─────┘       └──────────────────┘
                                 │
                                 ▼
                     ┌────────────────────────┐
                     │ Yandex Uniproxy WS API │
                     └────────────────────────┘
```

* **`src/`** — Основные сервисы на Rust (`capsula-alice`, `capsula-yandex`, `capsula-audio`).
* **`services/ring_daemon/`** — Драйвер управления RGB-кольцом и сенсорной панелью (C).
* **`services/onboarding/`** — Встроенный веб-портал настройки Wi-Fi и аккаунта Яндекса (C).
* **`services/wakeword/`** — Оффлайн-нейросетевой детектор фразы активации (C++ / TFLM).
* **`models/`** — Квантованная модель споттера `model.tflite`.
* **`scripts/`** — Скрипты инициализации, супервизора и вспомогательные консольные утилиты.

---

## 🛠 Сборка

Для компиляции под целевую платформу `aarch64-linux-musl`:

### 1. Сервисы на Rust
```bash
cargo build --release --target aarch64-unknown-linux-musl
```

### 2. Сервисы C / C++ (с использованием Zig)
```bash
# ring_daemon
zig cc -target aarch64-linux-musl -O2 services/ring_daemon/ring_daemon.c -o ring_daemon -lpthread

# capsule_onboarding
zig cc -target aarch64-linux-musl -O2 services/onboarding/onboarding_daemon.c -o capsule_onboarding -lpthread

# wakewordd
zig c++ -target aarch64-linux-musl -O2 services/wakeword/wakewordd.cc -o wakewordd -lm
```

---

## 🚀 Установка на чистую колонку

Перед началом убедитесь, что колонка имеет root-доступ по SSH.

### 1. Отключение встроенного ассистента Маруся
На оригинальной прошивке демоны Маруси (`voice-agent` и `okcall`) запускаются через init-скрипты и блокируют доступ к аудио-захвату. В Buildroot init-скрипты подключаются через `source` (`. $i`), а файловая система корня работает через OverlayFS.

> [!IMPORTANT]
> **Не переименовывайте** скрипты через `mv`: на UBIFS в OverlayFS это создаёт белые метки (whiteouts), что может повредить индекс файловой системы. Правильный способ — перетереть скрипты безопасными заглушками:

```bash
# Создание безопасных заглушек для Маруси
cat << 'EOF' > /etc/init.d/S97voice-agent.sh
#!/bin/sh
exit 0
EOF
chmod 755 /etc/init.d/S97voice-agent.sh

cat << 'EOF' > /etc/init.d/S97okcall.sh
#!/bin/sh
exit 0
EOF
chmod 755 /etc/init.d/S97okcall.sh

# Остановка текущих процессов Маруси
killall -9 voice-agent okcall 2>/dev/null
```

### 2. Настройка имени хоста и mDNS (`alice.local`)
Чтобы колонка сразу отзывалась по удобному адресу `http://alice.local`:
```bash
echo "alice" > /etc/hostname
hostname alice 2>/dev/null
mkdir -p /etc/avahi/services
cat << 'EOF' > /etc/avahi/services/http.service
<?xml version="1.0" standalone='no'?>
<!DOCTYPE service-group SYSTEM "avahi-service.dtd">
<service-group>
  <name replace-wildcards="yes">%h</name>
  <service>
    <type>_http._tcp</type>
    <port>80</port>
  </service>
</service-group>
EOF
```

### 3. Размещение исполняемых файлов
Скопируйте бинарники прошивки в `/usr/bin/`:
```bash
cp capsula-alice capsula-yandex capsula-audio wakewordd ring_daemon capsule_onboarding /usr/bin/
chmod +x /usr/bin/capsula-* /usr/bin/wakewordd /usr/bin/ring_daemon /usr/bin/capsule_onboarding
```

### 4. Размещение модели нейросети
```bash
mkdir -p /data
cp models/model.tflite /data/model.tflite
chmod 644 /data/model.tflite
```

### 5. Установка вспомогательных скриптов и автозапуска
Скопируйте скрипты управления в `/usr/bin/` и init-сервисы в `/etc/init.d/`:
```bash
cp scripts/capsula-alice-launcher.sh scripts/wifi_setup_helper.sh scripts/capsule-yandex-account scripts/capsule-mic /usr/bin/
chmod +x /usr/bin/capsula-alice-launcher.sh /usr/bin/wifi_setup_helper.sh /usr/bin/capsule-yandex-account /usr/bin/capsule-mic

cp scripts/S99capsula-alice scripts/S50avahi-daemon /etc/init.d/
chmod +x /etc/init.d/S99capsula-alice /etc/init.d/S50avahi-daemon

# Если файлы передавались из Windows-среды, нормализуйте окончания строк Unix (LF):
sed -i 's/\r$//' /usr/bin/capsula-* /usr/bin/wifi_setup_helper.sh /usr/bin/capsule-* /etc/init.d/S* 2>/dev/null
```

### 6. Запуск супервизора Алисы
```bash
/etc/init.d/S99capsula-alice start
```
Все службы (распознавание речи, светодиодное кольцо, Яндекс Музыка и веб-портал) запустятся автоматически. При последующих перезагрузках колонка будет стартовать сразу в режиме Алисы.

---

## 📱 Первоначальная настройка и смена аккаунта

1. **Подключение к Wi-Fi:**
   * При отсутствии домашней сети включите на смартфоне точку доступа с именем **`Setup`** (без пароля). Колонка подключится к ней автоматически.
   * Откройте в браузере страницу **`http://alice.local`** или локальный IP-адрес колонки.
   * Выберите вашу домашнюю сеть Wi-Fi и сохраните настройки.

2. **Привязка Яндекс ID:**
   * В веб-интерфейсе нажмите **«Получить код для входа»**.
   * Откройте [ya.ru/device](https://ya.ru/device) и введите предложенный код.
   * Либо в консоли колонки выполните команду:
     ```bash
     capsule-yandex-account login
     ```

---

## 🔄 OTA-обновления

Колонка может обновляться «по воздуху» из публичных raw-файлов GitHub без токена на устройстве. Манифест и бинарники привязаны к подвижному тегу `ota-latest`.

```bash
# однократная установка клиента
adb push ota/capsula-ota.sh /usr/bin/capsula-ota
adb shell chmod 755 /usr/bin/capsula-ota
adb push ota/S98capsula-ota /etc/init.d/S98capsula-ota
adb shell chmod 755 /etc/init.d/S98capsula-ota

# проверка и установка обновления
adb shell /usr/bin/capsula-ota check
adb shell /usr/bin/capsula-ota status
```

Клиент проверяет SHA-256 и размер каждого файла, ставит файлы через временные `.new`-файлы с атомарной заменой, делает резервную копию и умеет откатываться командой `capsula-ota rollback`. Отдельный демон `capsula-ota daemon` проверяет обновления каждые 10 минут и стартует при загрузке независимо от Алисы и PulseAudio. В ночное время (23:00–07:00) перезапуск сервисов откладывается до следующей проверки днём; звук и раз громкости при обновлении не включаются.

Подробности и порядок публикации релиза: [`ota/README.md`](ota/README.md).

---

## 📄 Лицензия

Проект распространяется под свободной лицензией [MIT](LICENSE).
