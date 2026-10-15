package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"
)

var (
	tgMu      sync.Mutex
	lastTG    time.Time
	httpClient = &http.Client{Timeout: 10 * time.Second}
)

// notifyTelegram шлёт сообщение, но не чаще раза в 2 минуты (антиспам)
func notifyTelegram(cfg *Config, baseName string) {
	if cfg.TelegramToken == "" || cfg.TelegramChatID == "" {
		return
	}
	tgMu.Lock()
	if time.Since(lastTG) < 2*time.Minute {
		tgMu.Unlock()
		return
	}
	lastTG = time.Now()
	tgMu.Unlock()

	text := fmt.Sprintf("🚨 РЕЙД БАЗЫ: %s\n⏰ %s",
		baseName, time.Now().Format("02.01.2006 15:04:05"))

	api := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", cfg.TelegramToken)
	resp, err := httpClient.PostForm(api, url.Values{
		"chat_id": {cfg.TelegramChatID},
		"text":    {text},
	})
	if err != nil {
		log.Println("telegram: ошибка отправки:", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Printf("telegram: сервер вернул %d: %s", resp.StatusCode, body)
	}
}