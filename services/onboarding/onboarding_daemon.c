#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <errno.h>
#include <sys/stat.h>
#include <signal.h>

#define HTTP_PORT 8080
#define CLIENT_ID "23cabbbdc6cd418abb4b39c32c41195d"
#define CLIENT_SECRET "53bc75238f0c4d08a118e51fe9203300"
#define CRED_PATH "/etc/alice/yandex_credentials.json"
#define HOST_CRED_PATH "/root/.capsula-mini/yandex_credentials.json"

static const char *INDEX_HTML =
"<!DOCTYPE html>\n"
"<html lang='ru'>\n"
"<head>\n"
"<meta charset='utf-8'>\n"
"<meta name='viewport' content='width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no'>\n"
"<title>Настройка Капсулы с Алисой</title>\n"
"<style>\n"
"* { box-sizing: border-box; margin: 0; padding: 0; -webkit-tap-highlight-color: transparent; }\n"
"body {\n"
"  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;\n"
"  background: #0d0f17;\n"
"  color: #f1f3f9;\n"
"  min-height: 100vh;\n"
"  display: flex;\n"
"  flex-direction: column;\n"
"  align-items: center;\n"
"  justify-content: flex-start;\n"
"  padding: 20px 16px 40px;\n"
"}\n"
".container {\n"
"  width: 100%;\n"
"  max-width: 440px;\n"
"  display: flex;\n"
"  flex-direction: column;\n"
"  gap: 18px;\n"
"}\n"
".header {\n"
"  text-align: center;\n"
"  padding: 12px 0 6px;\n"
"}\n"
".logo-ring {\n"
"  width: 68px;\n"
"  height: 68px;\n"
"  margin: 0 auto 14px;\n"
"  border-radius: 50%;\n"
"  background: conic-gradient(from 180deg at 50% 50%, #7b2cbf 0deg, #9d4edd 90deg, #c77dff 180deg, #7b2cbf 360deg);\n"
"  box-shadow: 0 0 24px rgba(157, 78, 221, 0.45);\n"
"  position: relative;\n"
"  animation: pulse 3s infinite ease-in-out;\n"
"}\n"
".logo-ring::after {\n"
"  content: '';\n"
"  position: absolute;\n"
"  inset: 6px;\n"
"  background: #0d0f17;\n"
"  border-radius: 50%;\n"
"}\n"
"@keyframes pulse {\n"
"  0%, 100% { transform: scale(1); box-shadow: 0 0 24px rgba(157, 78, 221, 0.45); }\n"
"  50% { transform: scale(1.05); box-shadow: 0 0 36px rgba(199, 125, 255, 0.65); }\n"
"}\n"
"h1 { font-size: 22px; font-weight: 700; color: #fff; margin-bottom: 4px; }\n"
"p.sub { font-size: 13px; color: #8e95a5; }\n"
".card {\n"
"  background: #181b26;\n"
"  border: 1px solid #292d3e;\n"
"  border-radius: 18px;\n"
"  padding: 18px;\n"
"  box-shadow: 0 6px 20px rgba(0,0,0,0.25);\n"
"}\n"
".card-title {\n"
"  display: flex;\n"
"  align-items: center;\n"
"  gap: 10px;\n"
"  font-size: 15px;\n"
"  font-weight: 600;\n"
"  color: #fff;\n"
"  margin-bottom: 14px;\n"
"}\n"
".badge {\n"
"  width: 24px;\n"
"  height: 24px;\n"
"  border-radius: 50%;\n"
"  background: #7b2cbf;\n"
"  color: #fff;\n"
"  display: flex;\n"
"  align-items: center;\n"
"  justify-content: center;\n"
"  font-size: 12px;\n"
"  font-weight: 700;\n"
"}\n"
".form-group { margin-bottom: 12px; }\n"
"label {\n"
"  display: block;\n"
"  font-size: 13px;\n"
"  color: #a4abbd;\n"
"  margin-bottom: 6px;\n"
"}\n"
"select, input[type='text'], input[type='password'] {\n"
"  width: 100%;\n"
"  padding: 12px 14px;\n"
"  background: #232736;\n"
"  border: 1px solid #363b50;\n"
"  border-radius: 12px;\n"
"  color: #fff;\n"
"  font-size: 14px;\n"
"  outline: none;\n"
"  transition: border-color 0.2s;\n"
"}\n"
"select:focus, input:focus {\n"
"  border-color: #9d4edd;\n"
"}\n"
".btn {\n"
"  width: 100%;\n"
"  padding: 13px;\n"
"  border: none;\n"
"  border-radius: 12px;\n"
"  font-size: 14px;\n"
"  font-weight: 600;\n"
"  cursor: pointer;\n"
"  display: flex;\n"
"  align-items: center;\n"
"  justify-content: center;\n"
"  gap: 8px;\n"
"  transition: all 0.2s;\n"
"}\n"
".btn-primary {\n"
"  background: linear-gradient(135deg, #7b2cbf 0%, #9d4edd 100%);\n"
"  color: #fff;\n"
"  box-shadow: 0 4px 14px rgba(123, 44, 191, 0.4);\n"
"}\n"
".btn-primary:active { transform: scale(0.98); opacity: 0.9; }\n"
".btn-yandex {\n"
"  background: #ffcc00;\n"
"  color: #000;\n"
"  box-shadow: 0 4px 14px rgba(255, 204, 0, 0.3);\n"
"  text-decoration: none;\n"
"}\n"
".btn-yandex:active { transform: scale(0.98); opacity: 0.9; }\n"
".btn-outline {\n"
"  background: transparent;\n"
"  border: 1px solid #363b50;\n"
"  color: #a4abbd;\n"
"  padding: 7px 12px;\n"
"  font-size: 12px;\n"
"  border-radius: 8px;\n"
"  width: auto;\n"
"}\n"
".btn-danger-outline {\n"
"  background: transparent;\n"
"  border: 1px solid rgba(255, 71, 87, 0.4);\n"
"  color: #ff4757;\n"
"  padding: 9px 14px;\n"
"  font-size: 13px;\n"
"  border-radius: 10px;\n"
"  width: auto;\n"
"}\n"
".status-box {\n"
"  padding: 12px 14px;\n"
"  border-radius: 12px;\n"
"  font-size: 13px;\n"
"  margin-top: 12px;\n"
"  display: none;\n"
"}\n"
".status-success { background: rgba(46, 213, 115, 0.15); color: #2ed573; border: 1px solid rgba(46,213,115,0.3); display: block; }\n"
".status-pending { background: rgba(255, 171, 0, 0.15); color: #ffab00; border: 1px solid rgba(255,171,0,0.3); display: block; }\n"
".status-error { background: rgba(255, 71, 87, 0.15); color: #ff4757; border: 1px solid rgba(255,71,87,0.3); display: block; }\n"
".code-display {\n"
"  font-size: 28px;\n"
"  font-weight: 800;\n"
"  letter-spacing: 5px;\n"
"  color: #ffcc00;\n"
"  text-align: center;\n"
"  padding: 14px;\n"
"  background: #232736;\n"
"  border-radius: 12px;\n"
"  margin: 12px 0;\n"
"  font-family: monospace;\n"
"}\n"
".tip-box {\n"
"  background: rgba(157, 78, 221, 0.1);\n"
"  border-left: 3px solid #9d4edd;\n"
"  padding: 10px 12px;\n"
"  border-radius: 6px;\n"
"  font-size: 12px;\n"
"  color: #c77dff;\n"
"  line-height: 1.4;\n"
"  margin-top: 10px;\n"
"}\n"
"</style>\n"
"</head>\n"
"<body>\n"
"<div class='container'>\n"
"  <div class='header'>\n"
"    <div class='logo-ring'></div>\n"
"    <h1>Капсула с Алисой</h1>\n"
"    <p class='sub' id='net-status-sub'>Быстрая настройка устройства</p>\n"
"  </div>\n"
"\n"
"  <!-- БЛОК 1: WI-FI -->\n"
"  <div class='card'>\n"
"    <div class='card-title'>\n"
"      <div class='badge'>1</div>\n"
"      <span>Подключение к Wi-Fi</span>\n"
"    </div>\n"
"    <div class='form-group'>\n"
"      <div style='display:flex; justify-content:space-between; align-items:center; margin-bottom:6px;'>\n"
"        <label style='margin:0'>Выберите сеть Wi-Fi:</label>\n"
"        <button class='btn btn-outline' onclick='scanWifi()'>🔄 Обновить</button>\n"
"      </div>\n"
"      <select id='wifi-select'>\n"
"        <option value=''>Поиск сетей...</option>\n"
"      </select>\n"
"    </div>\n"
"    <div class='form-group'>\n"
"      <label>Пароль от сети:</label>\n"
"      <input type='password' id='wifi-pass' placeholder='Введите пароль'>\n"
"    </div>\n"
"    <button class='btn btn-primary' id='btn-wifi-connect' onclick='connectWifi()'>Подключить к Wi-Fi</button>\n"
"    <div id='wifi-status' class='status-box'></div>\n"
"    <div class='tip-box'>\n"
"      💡 <strong>Резервный вход:</strong> Точка доступа с телефона <code>Setup</code> (без пароля) всегда активна. При отсутствии домашней сети колонка подключается к ней автоматически.\n"
"    </div>\n"
"  </div>\n"
"\n"
"  <!-- БЛОК 2: ЯНДЕКС ID -->\n"
"  <div class='card'>\n"
"    <div class='card-title'>\n"
"      <div class='badge'>2</div>\n"
"      <span>Привязка Яндекс ID</span>\n"
"    </div>\n"
"    \n"
"    <!-- Уже привязан -->\n"
"    <div id='yandex-linked-box' style='display:none;'>\n"
"      <div class='status-box status-success' style='margin-top:0;'>\n"
"        <div>✅ Аккаунт привязан: <strong id='linked-user-name'>Пользователь</strong></div>\n"
"      </div>\n"
"      <div style='display:flex; justify-content:flex-end; margin-top:12px;'>\n"
"        <button class='btn btn-danger-outline' onclick='resetAccount()'>🔄 Сменить аккаунт Яндекса</button>\n"
"      </div>\n"
"    </div>\n"
"\n"
"    <!-- Форма входа -->\n"
"    <div id='yandex-login-form'>\n"
"      <p style='font-size:13px; color:#a4abbd; margin-bottom:14px; line-height:1.4;'>\n"
"        Чтобы Алиса воспроизводила вашу музыку и управляла умным домом, подтвердите вход в Яндекс ID.\n"
"      </p>\n"
"      <button class='btn btn-yandex' id='btn-get-code' onclick='getYandexCode()'>Получить код для входа</button>\n"
"      <div id='yandex-code-block' style='display:none;'>\n"
"        <div class='code-display' id='user-code'>----</div>\n"
"        <a id='yandex-link' href='https://ya.ru/device' target='_blank' class='btn btn-yandex' style='margin-bottom:10px;'>Перейти на ya.ru/device</a>\n"
"        <p style='font-size:12px; color:#8e95a5; text-align:center;'>Откройте ссылку и введите код выше</p>\n"
"      </div>\n"
"    </div>\n"
"    <div id='yandex-status' class='status-box'></div>\n"
"  </div>\n"
"\n"
"  <!-- БЛОК 3: ЗАВЕРШЕНИЕ -->\n"
"  <div class='card' style='text-align:center;'>\n"
"    <button class='btn btn-primary' id='btn-finish' onclick='finishSetup()' style='background:#2ed573; box-shadow:0 4px 16px rgba(46,213,115,0.3);'>Завершить настройку</button>\n"
"  </div>\n"
"</div>\n"
"\n"
"<script>\n"
"let pollTimer = null;\n"
"let currentDeviceCode = '';\n"
"\n"
"function checkStatus() {\n"
"  fetch('/api/status')\n"
"    .then(r => r.json())\n"
"    .then(d => {\n"
"      if (d.ip) {\n"
"        document.getElementById('net-status-sub').textContent = `Подключено: ${d.ssid || 'Wi-Fi'} (${d.ip})`;\n"
"      }\n"
"      if (d.has_token) {\n"
"        document.getElementById('yandex-login-form').style.display = 'none';\n"
"        document.getElementById('yandex-linked-box').style.display = 'block';\n"
"        document.getElementById('linked-user-name').textContent = d.user_name || 'Яндекс пользователь';\n"
"      } else {\n"
"        document.getElementById('yandex-login-form').style.display = 'block';\n"
"        document.getElementById('yandex-linked-box').style.display = 'none';\n"
"      }\n"
"    })\n"
"    .catch(() => {});\n"
"}\n"
"\n"
"function scanWifi() {\n"
"  const sel = document.getElementById('wifi-select');\n"
"  sel.innerHTML = '<option>Сканирование...</option>';\n"
"  fetch('/api/scan')\n"
"    .then(r => r.json())\n"
"    .then(data => {\n"
"      sel.innerHTML = '';\n"
"      if (!data || data.length === 0) {\n"
"        sel.innerHTML = '<option value=\"\">Сети не найдены</option>';\n"
"        return;\n"
"      }\n"
"      data.forEach(item => {\n"
"        const opt = document.createElement('option');\n"
"        opt.value = item.ssid;\n"
"        const icon = item.signal > -55 ? '📶 Отличный' : (item.signal > -75 ? '📶 Хороший' : '📶 Слабый');\n"
"        opt.textContent = `${item.ssid} (${icon})`;\n"
"        sel.appendChild(opt);\n"
"      });\n"
"    })\n"
"    .catch(() => { sel.innerHTML = '<option value=\"\">Ошибка сканирования</option>'; });\n"
"}\n"
"\n"
"function connectWifi() {\n"
"  const ssid = document.getElementById('wifi-select').value;\n"
"  const pass = document.getElementById('wifi-pass').value;\n"
"  const status = document.getElementById('wifi-status');\n"
"  if (!ssid) { alert('Выберите сеть'); return; }\n"
"  status.className = 'status-box status-pending';\n"
"  status.textContent = `Подключение к «${ssid}»... Пожалуйста, подождите`;\n"
"  fetch('/api/connect', {\n"
"    method: 'POST',\n"
"    headers: {'Content-Type': 'application/json'},\n"
"    body: JSON.stringify({ssid: ssid, password: pass})\n"
"  })\n"
"  .then(r => r.json())\n"
"  .then(res => {\n"
"    if (res.status === 'ok') {\n"
"      status.className = 'status-box status-success';\n"
"      status.textContent = `Успешно подключено к «${ssid}»! IP: ${res.ip}`;\n"
"      checkStatus();\n"
"    } else {\n"
"      status.className = 'status-box status-error';\n"
"      status.textContent = res.msg || 'Ошибка подключения к Wi-Fi. Проверьте пароль.';\n"
"    }\n"
"  })\n"
"  .catch(() => {\n"
"    status.className = 'status-box status-error';\n"
"    status.textContent = 'Сетевая ошибка при запросе';\n"
"  });\n"
"}\n"
"\n"
"function getYandexCode() {\n"
"  const btn = document.getElementById('btn-get-code');\n"
"  btn.disabled = true;\n"
"  btn.textContent = 'Получение кода...';\n"
"  fetch('/api/yandex_code')\n"
"    .then(r => r.json())\n"
"    .then(data => {\n"
"      btn.style.display = 'none';\n"
"      document.getElementById('yandex-code-block').style.display = 'block';\n"
"      document.getElementById('user-code').textContent = data.user_code;\n"
"      document.getElementById('yandex-link').href = data.verification_url || 'https://ya.ru/device';\n"
"      currentDeviceCode = data.device_code;\n"
"      startPolling(data.device_code);\n"
"    })\n"
"    .catch(() => {\n"
"      btn.disabled = false;\n"
"      btn.textContent = 'Попробовать снова';\n"
"      alert('Не удалось получить код. Убедитесь, что колонка подключена к интернету.');\n"
"    });\n"
"}\n"
"\n"
"function startPolling(devCode) {\n"
"  if (pollTimer) clearInterval(pollTimer);\n"
"  const status = document.getElementById('yandex-status');\n"
"  status.className = 'status-box status-pending';\n"
"  status.textContent = 'Ожидание подтверждения на ya.ru/device...';\n"
"  pollTimer = setInterval(() => {\n"
"    fetch(`/api/yandex_poll?code=${encodeURIComponent(devCode)}`)\n"
"      .then(r => r.json())\n"
"      .then(res => {\n"
"        if (res.status === 'ok') {\n"
"          clearInterval(pollTimer);\n"
"          document.getElementById('yandex-code-block').style.display = 'none';\n"
"          status.className = 'status-box status-success';\n"
"          status.textContent = `Успешно авторизован: ${res.user_name || 'Яндекс пользователь'}!`;\n"
"          checkStatus();\n"
"        }\n"
"      });\n"
"  }, 3000);\n"
"}\n"
"\n"
"function resetAccount() {\n"
"  if (!confirm('Вы уверены, что хотите отвязать текущий аккаунт Яндекса и привязать новый?')) return;\n"
"  if (pollTimer) clearInterval(pollTimer);\n"
"  fetch('/api/reset_account', { method: 'POST' })\n"
"    .then(r => r.json())\n"
"    .then(res => {\n"
"      if (res.status === 'ok') {\n"
"        const btn = document.getElementById('btn-get-code');\n"
"        btn.style.display = 'block';\n"
"        btn.disabled = false;\n"
"        btn.textContent = 'Получить код для входа';\n"
"        document.getElementById('yandex-code-block').style.display = 'none';\n"
"        const st = document.getElementById('yandex-status');\n"
"        st.className = 'status-box';\n"
"        st.textContent = '';\n"
"        checkStatus();\n"
"      }\n"
"    })\n"
"    .catch(() => { alert('Ошибка при отвязке аккаунта'); });\n"
"}\n"
"\n"
"function finishSetup() {\n"
"  fetch('/api/finish', { method: 'POST' })\n"
"    .then(r => r.json())\n"
"    .then(() => {\n"
"      alert('Настройка завершена! Алиса готова к общению.');\n"
"    });\n"
"}\n"
"\n"
"window.onload = function() {\n"
"  checkStatus();\n"
"  scanWifi();\n"
"};\n"
"</script>\n"
"</body>\n"
"</html>\n";

static void send_response(int client_fd, int status, const char *content_type, const char *body, size_t body_len) {
    char header[512];
    int hlen = snprintf(header, sizeof(header),
        "HTTP/1.1 %d OK\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %zu\r\n"
        "Connection: close\r\n"
        "Access-Control-Allow-Origin: *\r\n"
        "\r\n", status, content_type, body_len);
    write(client_fd, header, hlen);
    if (body_len > 0) {
        write(client_fd, body, body_len);
    }
}

static void send_redirect(int client_fd, const char *location) {
    char header[512];
    int hlen = snprintf(header, sizeof(header),
        "HTTP/1.1 302 Found\r\n"
        "Location: %s\r\n"
        "Content-Length: 0\r\n"
        "Connection: close\r\n"
        "\r\n", location);
    write(client_fd, header, hlen);
}

// Handler for HTTP requests
static void handle_client(int client_fd) {
    char buf[4096];
    ssize_t n = read(client_fd, buf, sizeof(buf) - 1);
    if (n <= 0) {
        close(client_fd);
        return;
    }
    buf[n] = '\0';

    char method[16], uri[256];
    if (sscanf(buf, "%15s %255s", method, uri) < 2) {
        close(client_fd);
        return;
    }

    // Captive portal redirects
    if (strstr(uri, "generate_204") || strstr(uri, "hotspot-detect") || strstr(uri, "ncsi.txt") || strstr(uri, "connecttest")) {
        send_redirect(client_fd, "http://192.168.4.1:8080/");
        close(client_fd);
        return;
    }

    // Root page
    if (strcmp(uri, "/") == 0 || strcmp(uri, "/index.html") == 0) {
        send_response(client_fd, 200, "text/html; charset=utf-8", INDEX_HTML, strlen(INDEX_HTML));
        close(client_fd);
        return;
    }

    // 1. API: Scan Wi-Fi networks
    if (strcmp(uri, "/api/scan") == 0) {
        system("wpa_cli -i wlan0 scan >/dev/null 2>&1");
        usleep(1200000); // 1.2s wait
        FILE *fp = popen("wpa_cli -i wlan0 scan_results", "r");
        char line[256];
        char json[4096];
        int jlen = snprintf(json, sizeof(json), "[");
        int count = 0;
        char seen_ssids[32][64];
        int seen_count = 0;

        if (fp) {
            if (fgets(line, sizeof(line), fp)) {
                while (fgets(line, sizeof(line), fp)) {
                    char bssid[32], flags[128], ssid[64];
                    int freq = 0, signal = 0;
                    if (sscanf(line, "%31s %d %d %127s %63[^\r\n]", bssid, &freq, &signal, flags, ssid) >= 5) {
                        if (strlen(ssid) == 0) continue;
                        int duplicate = 0;
                        for (int i = 0; i < seen_count; i++) {
                            if (strcmp(seen_ssids[i], ssid) == 0) {
                                duplicate = 1;
                                break;
                            }
                        }
                        if (duplicate) continue;
                        if (seen_count < 32) {
                            strncpy(seen_ssids[seen_count++], ssid, 63);
                        }
                        if (count > 0) jlen += snprintf(json + jlen, sizeof(json) - jlen, ",");
                        jlen += snprintf(json + jlen, sizeof(json) - jlen,
                            "{\"ssid\":\"%s\",\"signal\":%d,\"secure\":%s}",
                            ssid, signal, strstr(flags, "WPA") ? "true" : "false");
                        count++;
                        if (count >= 20) break;
                    }
                }
            }
            pclose(fp);
        }
        snprintf(json + jlen, sizeof(json) - jlen, "]");
        send_response(client_fd, 200, "application/json", json, strlen(json));
        close(client_fd);
        return;
    }

    // 2. API: Connect to Wi-Fi
    if (strcmp(uri, "/api/connect") == 0 && strcmp(method, "POST") == 0) {
        char *body = strstr(buf, "\r\n\r\n");
        char ssid[64] = {0}, pass[64] = {0};
        if (body) {
            body += 4;
            char *s_ptr = strstr(body, "\"ssid\":\"");
            if (s_ptr) {
                sscanf(s_ptr + 8, "%63[^\"]", ssid);
            }
            char *p_ptr = strstr(body, "\"password\":\"");
            if (p_ptr) {
                sscanf(p_ptr + 12, "%63[^\"]", pass);
            }
        }
        if (strlen(ssid) > 0) {
            // Write wpa_supplicant.conf with Setup prioritized first (priority 100), and user Wi-Fi (priority 10)
            FILE *wcf = fopen("/etc/wpa_supplicant.conf", "w");
            if (wcf) {
                fprintf(wcf,
                    "ctrl_interface=/var/run/wpa_supplicant\n"
                    "update_config=1\n\n"
                    "network={\n"
                    "\tssid=\"Setup\"\n"
                    "\tscan_ssid=1\n"
                    "\tkey_mgmt=NONE\n"
                    "\tpriority=100\n"
                    "}\n\n"
                    "network={\n"
                    "\tssid=\"%s\"\n"
                    "\tscan_ssid=1\n"
                    "\tpsk=\"%s\"\n"
                    "\tkey_mgmt=WPA-PSK\n"
                    "\tpriority=10\n"
                    "}\n",
                    ssid, pass);
                fclose(wcf);
            }

            // Reconfigure wpa_supplicant
            system("wpa_cli -i wlan0 reconfigure >/dev/null 2>&1 &");

            // Wait up to 10 seconds for IP
            char ip[32] = {0};
            for (int w = 0; w < 10; w++) {
                sleep(1);
                FILE *p = popen("ip -4 addr show wlan0 2>/dev/null | grep -o 'inet [0-9.]*' | cut -d' ' -f2", "r");
                if (p) {
                    if (fgets(ip, sizeof(ip), p)) {
                        char *nl = strchr(ip, '\n');
                        if (nl) *nl = '\0';
                    }
                    pclose(p);
                }
                if (strlen(ip) > 0 && strcmp(ip, "127.0.0.1") != 0) {
                    break;
                }
            }

            char resp[128];
            if (strlen(ip) > 0) {
                snprintf(resp, sizeof(resp), "{\"status\":\"ok\",\"ip\":\"%s\"}", ip);
            } else {
                snprintf(resp, sizeof(resp), "{\"status\":\"error\",\"msg\":\"Не удалось подключиться к Wi-Fi. Проверьте пароль.\"}");
            }
            send_response(client_fd, 200, "application/json", resp, strlen(resp));
        } else {
            send_response(client_fd, 400, "application/json", "{\"status\":\"error\",\"msg\":\"Empty SSID\"}", 34);
        }
        close(client_fd);
        return;
    }

    // 2b. API: Status check
    if (strcmp(uri, "/api/status") == 0) {
        FILE *p = popen("ip -4 addr show wlan0 2>/dev/null | grep -o 'inet [0-9.]*' | cut -d' ' -f2", "r");
        char ip[32] = {0};
        if (p) {
            if (fgets(ip, sizeof(ip), p)) {
                char *nl = strchr(ip, '\n');
                if (nl) *nl = '\0';
            }
            pclose(p);
        }

        char cur_ssid[64] = "Wi-Fi";
        FILE *sp = popen("wpa_cli -i wlan0 status 2>/dev/null | grep '^ssid=' | cut -d'=' -f2", "r");
        if (sp) {
            if (fgets(cur_ssid, sizeof(cur_ssid), sp)) {
                char *nl = strchr(cur_ssid, '\n');
                if (nl) *nl = '\0';
            }
            pclose(sp);
        }

        int has_token = ((access("/etc/alice/yandex_credentials.json", F_OK) == 0) ||
                         (access("/data/capsula-yandex/credentials.json", F_OK) == 0)) ? 1 : 0;
        char user_name[64] = "Пользователь";

        if (has_token) {
            FILE *f = fopen("/etc/alice/yandex_credentials.json", "r");
            if (!f) f = fopen("/data/capsula-yandex/credentials.json", "r");
            if (f) {
                char cred_buf[2048] = {0};
                fread(cred_buf, 1, sizeof(cred_buf) - 1, f);
                fclose(f);
                char *fn = strstr(cred_buf, "\"first_name\": \"");
                if (!fn) fn = strstr(cred_buf, "\"first_name\":\"");
                if (fn) {
                    fn = strchr(fn, ':');
                    while (*fn == ' ' || *fn == ':' || *fn == '\"') fn++;
                    sscanf(fn, "%63[^\"]", user_name);
                } else {
                    char *dn = strstr(cred_buf, "\"display_name\": \"");
                    if (!dn) dn = strstr(cred_buf, "\"display_name\":\"");
                    if (dn) {
                        dn = strchr(dn, ':');
                        while (*dn == ' ' || *dn == ':' || *dn == '\"') dn++;
                        sscanf(dn, "%63[^\"]", user_name);
                    }
                }
            }
        }

        char sresp[384];
        snprintf(sresp, sizeof(sresp),
            "{\"status\":\"ok\",\"mode\":\"STA\",\"ip\":\"%s\",\"ssid\":\"%s\",\"has_token\":%s,\"user_name\":\"%s\"}",
            ip, cur_ssid, has_token ? "true" : "false", user_name);
        send_response(client_fd, 200, "application/json", sresp, strlen(sresp));
        close(client_fd);
        return;
    }

    // 2c. API: Reset Yandex Account
    if (strcmp(uri, "/api/reset_account") == 0 && strcmp(method, "POST") == 0) {
        unlink("/etc/alice/yandex_credentials.json");
        unlink("/data/capsula-yandex/credentials.json");
        unlink("/root/.capsula-mini/yandex_credentials.json");
        unlink("/data/capsula-hub/credentials.json");
        unlink("/data/yandex_credentials.json");
        unlink("/tmp/yandex_credentials.json");
        unlink("/tmp/capsula-yandex-player-state.json");
        system("killall -9 capsula-alice capsula-yandex 2>/dev/null");
        send_response(client_fd, 200, "application/json", "{\"status\":\"ok\"}", 15);
        close(client_fd);
        return;
    }

    // 3. API: Request Yandex Device Code
    if (strcmp(uri, "/api/yandex_code") == 0) {
        char cmd[512];
        snprintf(cmd, sizeof(cmd),
            "wget -q -O - -T 15 --post-data 'client_id=" CLIENT_ID "&device_name=CapsuleMiniAlice' "
            "https://oauth.yandex.ru/device/code");
        FILE *fp = popen(cmd, "r");
        char resp[1024] = {0};
        if (fp) {
            fread(resp, 1, sizeof(resp) - 1, fp);
            pclose(fp);
        }
        send_response(client_fd, 200, "application/json", resp, strlen(resp));
        close(client_fd);
        return;
    }

    // 4. API: Poll Yandex Token
    if (strncmp(uri, "/api/yandex_poll?code=", 22) == 0) {
        char dev_code[128] = {0};
        sscanf(uri + 22, "%127[^& \r\n]", dev_code);
        char cmd[1024];
        snprintf(cmd, sizeof(cmd),
            "wget -q -O - -T 15 --post-data 'grant_type=device_code&code=%s&client_id=" CLIENT_ID "&client_secret=" CLIENT_SECRET "' "
            "https://oauth.yandex.ru/token",
            dev_code);
        FILE *fp = popen(cmd, "r");
        char resp[2048] = {0};
        if (fp) {
            fread(resp, 1, sizeof(resp) - 1, fp);
            pclose(fp);
        }

        if (strstr(resp, "\"access_token\"")) {
            // Extract token
            char token[512] = {0};
            char *t_ptr = strstr(resp, "\"access_token\": \"");
            if (!t_ptr) t_ptr = strstr(resp, "\"access_token\":\"");
            if (t_ptr) {
                t_ptr = strchr(t_ptr, ':');
                if (t_ptr) {
                    while (*t_ptr == ' ' || *t_ptr == ':' || *t_ptr == '\"') t_ptr++;
                    sscanf(t_ptr, "%511[^\"]", token);
                }
            }

            char refresh_token[512] = {0};
            char *r_ptr = strstr(resp, "\"refresh_token\": \"");
            if (!r_ptr) r_ptr = strstr(resp, "\"refresh_token\":\"");
            if (r_ptr) {
                r_ptr = strchr(r_ptr, ':');
                if (r_ptr) {
                    while (*r_ptr == ' ' || *r_ptr == ':' || *r_ptr == '\"') r_ptr++;
                    sscanf(r_ptr, "%511[^\"]", refresh_token);
                }
            }

            // Fetch user info
            char user_name[64] = "Пользователь";
            if (strlen(token) > 0) {
                char user_cmd[512];
                snprintf(user_cmd, sizeof(user_cmd),
                    "wget -q -O - -T 15 --header 'Authorization: OAuth %s' "
                    "'https://login.yandex.ru/info?format=json'", token);
                FILE *uf = popen(user_cmd, "r");
                if (uf) {
                    char ubuf[1024] = {0};
                    fread(ubuf, 1, sizeof(ubuf) - 1, uf);
                    pclose(uf);
                    char *fn = strstr(ubuf, "\"first_name\": \"");
                    if (!fn) fn = strstr(ubuf, "\"first_name\":\"");
                    if (fn) {
                        fn = strchr(fn, ':');
                        while (*fn == ' ' || *fn == ':' || *fn == '\"') fn++;
                        sscanf(fn, "%63[^\"]", user_name);
                    } else {
                        char *dn = strstr(ubuf, "\"display_name\": \"");
                        if (!dn) dn = strstr(ubuf, "\"display_name\":\"");
                        if (dn) {
                            dn = strchr(dn, ':');
                            while (*dn == ' ' || *dn == ':' || *dn == '\"') dn++;
                            sscanf(dn, "%63[^\"]", user_name);
                        }
                    }
                }
            }

            // Save credentials to all standard paths
            system("mkdir -p /etc/alice /root/.capsula-mini /data/capsula-yandex");
            const char *cred_paths[] = {
                "/etc/alice/yandex_credentials.json",
                "/data/capsula-yandex/credentials.json",
                "/root/.capsula-mini/yandex_credentials.json",
                NULL
            };
            for (int i = 0; cred_paths[i]; i++) {
                FILE *cf = fopen(cred_paths[i], "w");
                if (cf) {
                    fprintf(cf, "{\"access_token\":\"%s\",\"refresh_token\":\"%s\",\"first_name\":\"%s\",\"expires_in\":31536000}\n",
                            token, refresh_token, user_name);
                    fclose(cf);
                    chmod(cred_paths[i], 0600);
                }
            }

            // Clean any stale playback state
            unlink("/tmp/capsula-yandex-player-state.json");

            // Restart both Yandex Music daemon and Alice assistant
            system("killall -9 capsula-alice capsula-yandex 2>/dev/null; sleep 1; /usr/bin/capsula-yandex > /tmp/capsula-yandex.log 2>&1 & /etc/init.d/S99capsula-alice start >/dev/null 2>&1 &");

            char ok_resp[256];
            snprintf(ok_resp, sizeof(ok_resp), "{\"status\":\"ok\",\"user_name\":\"%s\"}", user_name);
            send_response(client_fd, 200, "application/json", ok_resp, strlen(ok_resp));
        } else {
            send_response(client_fd, 200, "application/json", "{\"status\":\"pending\"}", 20);
        }
        close(client_fd);
        return;
    }

    // 5. API: Finish Setup
    if (strcmp(uri, "/api/finish") == 0 && strcmp(method, "POST") == 0) {
        system("/etc/init.d/S99capsula-alice start >/dev/null 2>&1 &");
        send_response(client_fd, 200, "application/json", "{\"status\":\"done\"}", 17);
        close(client_fd);
        return;
    }

    send_response(client_fd, 404, "text/plain", "Not Found", 9);
    close(client_fd);
}

static void *worker_thread(void *arg) {
    int client_fd = (int)(intptr_t)arg;
    handle_client(client_fd);
    return NULL;
}

static void *listener_thread(void *arg) {
    int port = (int)(intptr_t)arg;
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket");
        return NULL;
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "bind error on port %d: %s\n", port, strerror(errno));
        close(server_fd);
        return NULL;
    }

    if (listen(server_fd, 10) < 0) {
        fprintf(stderr, "listen error on port %d: %s\n", port, strerror(errno));
        close(server_fd);
        return NULL;
    }

    printf("[Onboarding] Portal listening on port %d...\n", port);
    fflush(stdout);

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t clen = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &clen);
        if (client_fd < 0) continue;

        pthread_t th;
        pthread_create(&th, NULL, worker_thread, (void *)(intptr_t)client_fd);
        pthread_detach(th);
    }

    close(server_fd);
    return NULL;
}

int main(int argc, char **argv) {
    (void)argc; (void)argv;
    signal(SIGPIPE, SIG_IGN);

    pthread_t th80;
    pthread_create(&th80, NULL, listener_thread, (void *)(intptr_t)80);
    pthread_detach(th80);

    listener_thread((void *)(intptr_t)HTTP_PORT);
    return 0;
}
