package lab

// Быстрая память рядом с базой (профиль cache, m09). Клиент написан руками и
// умеет ровно то, что нужно сценам: команда — ответ. Протокол Redis для этого
// достаточно мал, а лишняя зависимость в стенде стоит дороже сотни строк.
//
// Пул соединений здесь не украшение: сцена про давку на промахе кэша подаёт
// сотню одновременных читателей, и одно соединение на всех превратило бы её
// в сцену про очередь к кэшу.

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

type Redis struct {
	addr string
	pool chan *redisConn
}

type redisConn struct {
	c  net.Conn
	br *bufio.Reader
}

// Reply — ответ Redis в разобранном виде. Нужны все пять форм протокола:
// строка, число, отсутствие значения, ошибка и массив.
type Reply struct {
	Str string
	Int int64
	Nil bool
	Arr []Reply
}

func DialRedis(addr string, size int) (*Redis, error) {
	if size <= 0 {
		size = 8
	}
	r := &Redis{addr: addr, pool: make(chan *redisConn, size)}
	// Одно соединение открываем сразу: если кэша нет, узнать об этом лучше
	// здесь, а не в середине сцены.
	c, err := r.dial()
	if err != nil {
		return nil, err
	}
	r.put(c)
	return r, nil
}

// DialRedisWait ждёт кэш: в профиле он поднимается вместе со всеми, и первая
// сцена после старта иначе упиралась бы в гонку с healthcheck.
func DialRedisWait(addr string, size int, wait time.Duration) (*Redis, error) {
	deadline := time.Now().Add(wait)
	for {
		r, err := DialRedis(addr, size)
		if err == nil {
			return r, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("кэш недоступен за %s: %w", wait, err)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func (r *Redis) dial() (*redisConn, error) {
	c, err := net.DialTimeout("tcp", r.addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	return &redisConn{c: c, br: bufio.NewReader(c)}, nil
}

func (r *Redis) get() (*redisConn, error) {
	select {
	case c := <-r.pool:
		return c, nil
	default:
		return r.dial()
	}
}

func (r *Redis) put(c *redisConn) {
	select {
	case r.pool <- c:
	default:
		c.c.Close()
	}
}

func (r *Redis) Close() {
	for {
		select {
		case c := <-r.pool:
			c.c.Close()
		default:
			return
		}
	}
}

// Do отправляет команду и возвращает разобранный ответ.
func (r *Redis) Do(args ...string) (Reply, error) {
	c, err := r.get()
	if err != nil {
		return Reply{}, err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	_ = c.c.SetDeadline(time.Now().Add(30 * time.Second))
	if _, err := c.c.Write([]byte(b.String())); err != nil {
		c.c.Close()
		return Reply{}, err
	}
	rep, err := readReply(c.br)
	if err != nil {
		c.c.Close()
		return Reply{}, err
	}
	r.put(c)
	return rep, nil
}

func readReply(br *bufio.Reader) (Reply, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return Reply{}, err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return Reply{}, errors.New("пустой ответ кэша")
	}
	switch line[0] {
	case '+':
		return Reply{Str: line[1:]}, nil
	case '-':
		return Reply{}, errors.New(line[1:])
	case ':':
		n, err := strconv.ParseInt(line[1:], 10, 64)
		return Reply{Int: n}, err
	case '$':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return Reply{}, err
		}
		if n < 0 {
			return Reply{Nil: true}, nil
		}
		buf := make([]byte, n+2) // значение и CRLF
		if _, err := ioReadFull(br, buf); err != nil {
			return Reply{}, err
		}
		return Reply{Str: string(buf[:n])}, nil
	case '*':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return Reply{}, err
		}
		if n < 0 {
			return Reply{Nil: true}, nil
		}
		out := Reply{Arr: make([]Reply, 0, n)}
		for i := 0; i < n; i++ {
			item, err := readReply(br)
			if err != nil {
				return Reply{}, err
			}
			out.Arr = append(out.Arr, item)
		}
		return out, nil
	default:
		return Reply{}, fmt.Errorf("непонятный ответ кэша: %q", line)
	}
}

func ioReadFull(br *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := br.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// ── то, чем пользуются сцены ────────────────────────────────────────────────

func (r *Redis) Get(key string) (string, bool, error) {
	rep, err := r.Do("GET", key)
	if err != nil {
		return "", false, err
	}
	return rep.Str, !rep.Nil, nil
}

func (r *Redis) SetPX(key, value string, ttl time.Duration) error {
	_, err := r.Do("SET", key, value, "PX", strconv.FormatInt(ttl.Milliseconds(), 10))
	return err
}

// SetNX — взять аренду. Возвращает false, если ключ уже занят: это и есть
// «блокировку держит кто-то другой».
func (r *Redis) SetNX(key, value string, ttl time.Duration) (bool, error) {
	rep, err := r.Do("SET", key, value, "PX", strconv.FormatInt(ttl.Milliseconds(), 10), "NX")
	if err != nil {
		return false, err
	}
	return !rep.Nil, nil
}

func (r *Redis) Del(keys ...string) error {
	_, err := r.Do(append([]string{"DEL"}, keys...)...)
	return err
}

func (r *Redis) Incr(key string) (int64, error) {
	rep, err := r.Do("INCR", key)
	return rep.Int, err
}

func (r *Redis) FlushAll() error {
	_, err := r.Do("FLUSHALL")
	return err
}
