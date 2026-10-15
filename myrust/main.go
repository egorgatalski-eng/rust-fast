package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/faiface/beep"
	"github.com/faiface/beep/speaker"
	"github.com/faiface/beep/wav"
	"github.com/gorilla/websocket"
)

// ---------- Конфиг ----------

type Config struct {
	ServerIP    string            `json:"server_ip"`
	AppPort     int               `json:"app_port"`
	SteamID     uint64            `json:"steam_id"`
	PlayerToken int64             `json:"player_token"`
	SoundFile   string            `json:"sound_file"`
	AntiSpamSec int               `json:"antispam_sec"`
	Debug       bool              `json:"debug"`
	AlarmNames  map[uint32]string `json:"alarm_names"`
		TelegramToken  string `json:"telegram_token"`
	TelegramChatID string `json:"telegram_chat_id"`
}

func loadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cfg := &Config{AntiSpamSec: 30}
	if err := json.Unmarshal(b, cfg); err != nil {
		return nil, err
	}
	if cfg.ServerIP == "" || cfg.AppPort == 0 || cfg.SteamID == 0 || cfg.PlayerToken == 0 {
		return nil, fmt.Errorf("заполните server_ip, app_port, steam_id, player_token в config.json")
	}
	return cfg, nil
}

// ---------- Мини-protobuf (wire format) ----------

type protoReader struct {
	b   []byte
	pos int
	err error
}

func (r *protoReader) done() bool { return r.pos >= len(r.b) || r.err != nil }

func (r *protoReader) readVarint() uint64 {
	if r.err != nil {
		return 0
	}
	var x uint64
	var s uint
	for i := 0; ; i++ {
		if r.pos >= len(r.b) {
			r.err = io.ErrUnexpectedEOF
			return 0
		}
		c := r.b[r.pos]
		r.pos++
		if c < 0x80 {
			return x | uint64(c)<<s
		}
		x |= uint64(c&0x7f) << s
		s += 7
	}
}

func (r *protoReader) readBytes() []byte {
	if r.err != nil {
		return nil
	}
	n := int(r.readVarint())
	if r.err != nil || n < 0 || r.pos+n > len(r.b) {
		r.err = io.ErrUnexpectedEOF
		return nil
	}
	b := r.b[r.pos : r.pos+n]
	r.pos += n
	return b
}

func (r *protoReader) skip(wire int) {
	if r.err != nil {
		return
	}
	switch wire {
	case 0:
		r.readVarint()
	case 1:
		r.pos += 8
	case 2:
		r.readBytes()
	case 5:
		r.pos += 4
	default:
		r.err = fmt.Errorf("неизвестный wire type %d", wire)
	}
	if r.pos > len(r.b) {
		r.err = io.ErrUnexpectedEOF
	}
}

func putUvarint(dst []byte, v uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	return append(dst, buf[:n]...)
}

func fieldVarint(dst []byte, num int, v uint64) []byte {
	dst = putUvarint(dst, uint64(num<<3))
	return putUvarint(dst, v)
}

func fieldBytes(dst []byte, num int, b []byte) []byte {
	dst = putUvarint(dst, uint64(num<<3|2))
	dst = putUvarint(dst, uint64(len(b)))
	return append(dst, b...)
}

// buildGetInfo — AppRequest { seq, playerId, playerToken, getInfo: {} }
func buildGetInfo(seq uint32, playerID uint64, token int64) []byte {
	m := fieldVarint(nil, 1, uint64(seq))
	m = fieldVarint(m, 2, playerID)
	m = fieldVarint(m, 3, uint64(token)) // uint64() даст two's complement для отрицательных токенов
	m = fieldBytes(m, 4, nil)            // getInfo: {}
	return m
}

// ---------- Событие Smart Alarm ----------

const entityTypeAlarm = 1 // Alarm в rustplus.proto (Switch=0, Alarm=1, StorageMonitor=2)

type EntityChanged struct {
	EntityID uint32
	Value    bool
	Type     int
	Payload  string
}

func parseEntityChanged(b []byte) *EntityChanged {
	ec := &EntityChanged{}
	r := &protoReader{b: b}
	for !r.done() {
		tag := r.readVarint()
		num, wire := int(tag>>3), int(tag&7)
		switch {
		case num == 1 && wire == 0:
			ec.EntityID = uint32(r.readVarint())
		case num == 2 && wire == 0:
			ec.Value = r.readVarint() != 0
		case num == 3 && wire == 0:
			ec.Type = int(r.readVarint())
		case num == 4 && wire == 2:
			ec.Payload = string(r.readBytes())
		default:
			r.skip(wire)
		}
	}
	if r.err != nil {
		return nil
	}
	return ec
}

// parseAppMessage ищет broadcast.entityChanged внутри AppMessage
func parseAppMessage(b []byte) *EntityChanged {
	r := &protoReader{b: b}
	for !r.done() {
		tag := r.readVarint()
		num, wire := int(tag>>3), int(tag&7)
		if num == 1 && wire == 2 { // broadcast
			inner := r.readBytes()
			ir := &protoReader{b: inner}
			for !ir.done() {
				t2 := ir.readVarint()
				n2, w2 := int(t2>>3), int(t2&7)
				if n2 == 3 && w2 == 2 { // entityChanged
					return parseEntityChanged(ir.readBytes())
				}
				ir.skip(w2)
			}
			return nil
		}
		r.skip(wire)
	}
	return nil
}

// ---------- Звук ----------

var (
	soundMu   sync.Mutex
	lastSound time.Time
)

func playSiren(cfg *Config) {
	soundMu.Lock()
	if time.Since(lastSound) < time.Duration(cfg.AntiSpamSec)*time.Second {
		soundMu.Unlock()
		log.Println("⚠️  Повторная тревога проигнорирована (антиспам)")
		return
	}
	lastSound = time.Now()
	soundMu.Unlock()

	for i := 0; i < 3; i++ { // сирена 3 раза
		f, err := os.Open(cfg.SoundFile)
		if err != nil {
			log.Println("ошибка звука:", err)
			return
		}
		streamer, format, err := wav.Decode(f)
		if err != nil {
			log.Println("ошибка wav:", err)
			f.Close()
			return
		}
		done := make(chan struct{})
		speaker.Play(beep.Seq(streamer, beep.Callback(func() {
			close(done)
			f.Close()
		})))
		<-done
	}
}

// ---------- Соединение ----------

func readFrame(conn *websocket.Conn) ([]byte, error) {
	// каждый WebSocket-фрейм: 4 байта длины (big-endian) + protobuf-пакет
	_, msg, err := conn.ReadMessage()
	if err != nil {
		return nil, err
	}
	if len(msg) < 4 {
		return nil, fmt.Errorf("короткий пакет (%d байт)", len(msg))
	}
	n := int(binary.BigEndian.Uint32(msg[:4]))
	if n <= 0 || n > len(msg)-4 {
		return nil, fmt.Errorf("некорректная длина пакета: %d", n)
	}
	return msg[4 : 4+n], nil
}

func writeFrame(conn *websocket.Conn, payload []byte) error {
	buf := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(payload)))
	copy(buf[4:], payload)
	return conn.WriteMessage(websocket.BinaryMessage, buf)
}

func connectAndListen(cfg *Config) error {
	url := fmt.Sprintf("ws://%s:%d/%d/%d", net.ParseIP(cfg.ServerIP), cfg.AppPort, cfg.SteamID, cfg.PlayerToken)
	if ip := net.ParseIP(cfg.ServerIP); ip == nil { // если ввели домен
		url = fmt.Sprintf("ws://%s:%d/%d/%d", cfg.ServerIP, cfg.AppPort, cfg.SteamID, cfg.PlayerToken)
	}
	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	log.Println("✅ WebSocket подключён, жду события...")

	// авторизация/проверка токена
	if err := writeFrame(conn, buildGetInfo(1, cfg.SteamID, cfg.PlayerToken)); err != nil {
		return err
	}

	// keep-alive: раз в 30 сек, чтобы соединение не умирало молча
	var seq uint32 = 2
	ping := time.NewTicker(30 * time.Second)
	defer ping.Stop()
	go func() {
		for range ping.C {
			if err := writeFrame(conn, buildGetInfo(seq, cfg.SteamID, cfg.PlayerToken)); err != nil {
				return
			}
			seq++
		}
	}()

	conn.SetReadLimit(1 << 20)
	for {
		payload, err := readFrame(conn)
		if err != nil {
			return err
		}
		ec := parseAppMessage(payload)
		if ec == nil {
			continue
		}
		name := cfg.AlarmNames[ec.EntityID]
		if name == "" {
			name = fmt.Sprintf("Alarm#%d", ec.EntityID)
		}
		if cfg.Debug {
			log.Printf("entityChanged: id=%d type=%d value=%v payload=%q", ec.EntityID, ec.Type, ec.Value, ec.Payload)
		}
		if ec.Type == entityTypeAlarm {
			log.Printf("🚨 ТРЕВОГА: %s (%s)", name, time.Now().Format("15:04:05"))
			go playSiren(cfg)
		}
	}
}

func main() {
	cfg, err := loadConfig("config.json")
	if err != nil {
		log.Fatal("config: ", err)
	}

	// проверка и инициализация звука (один раз)
	f, err := os.Open(cfg.SoundFile)
	if err != nil {
		log.Fatalf("не найден файл звука %q: %v", cfg.SoundFile, err)
	}
	_, format, err := wav.Decode(f)
	if err != nil {
		log.Fatalf("не удалось декодировать %q: %v", cfg.SoundFile, err)
	}
	f.Close()
	speaker.Init(format.SampleRate, format.SampleRate.N(time.Second/10))

	delay := 10 * time.Second
	for {
		log.Printf("Подключение к %s:%d ...", cfg.ServerIP, cfg.AppPort)
		if err := connectAndListen(cfg); err != nil {
			log.Printf("⚠️  Соединение разорвано: %v", err)
		}
		log.Printf("Переподключение через %v...", delay)
		time.Sleep(delay)
		if delay < time.Minute {
			delay *= 2 // backoff: 10с → 20с → 40с → 60с
		}
	}
}