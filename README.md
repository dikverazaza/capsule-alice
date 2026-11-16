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

## 🚀 Установка на колонку

1. Скопируйте исполняемые файлы в `/usr/bin/`:
   ```bash
   cp capsula-alice capsula-yandex capsula-audio wakewordd ring_daemon capsule_onboarding /usr/bin/
   chmod +x /usr/bin/capsula-* /usr/bin/wakewordd /usr/bin/ring_daemon /usr/bin/capsule_onboarding
   ```
2. Разместите модель нейросети:
   ```bash
   mkdir -p /data
   cp models/model.tflite /data/model.tflite
   ```
3. Скопируйте скрипты управления:
   ```bash
   cp scripts/capsula-alice-launcher.sh scripts/wifi_setup_helper.sh scripts/capsule-yandex-account scripts/capsule-mic /usr/bin/
   chmod +x /usr/bin/capsula-alice-launcher.sh /usr/bin/wifi_setup_helper.sh /usr/bin/capsule-yandex-account /usr/bin/capsule-mic
   cp scripts/S99capsula-alice /etc/init.d/S99capsula-alice
   chmod +x /etc/init.d/S99capsula-alice
   ```
4. Запустите супервизор:
   ```bash
   /etc/init.d/S99capsula-alice start
   ```

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

## 📄 Лицензия

Проект распространяется под свободной лицензией [MIT](LICENSE).
