package lab

// Контекст пользователя, который едет вниз по цепочке (m13 l02). Периметр
// проверил токен и знает, чей это запрос; сервису за периметром это знание
// нужно, а токена у него уже нет. Едет контекст обычным заголовком — и ровно
// поэтому его подписывают: заголовок выставит кто угодно, кто дотянулся до
// сети, а подпись — только тот, у кого есть ключ.
//
// Ключ общий у периметра и у сервиса и задаётся окружением. Разбирать саму
// подпись курс не собирается: она нужна ровно настолько, чтобы «сосед по сети
// назвался чужим именем» отличалось от «периметр сказал, чей это запрос».

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"sync"
)

const (
	// HeaderUser — чьё намерение сейчас выполняется.
	HeaderUser = "X-Lab-User"
	// HeaderUserSig — подпись периметра под этим именем.
	HeaderUserSig = "X-Lab-User-Sig"
)

var (
	contextKeyOnce sync.Once
	contextKey     []byte
)

func trustKey() []byte {
	contextKeyOnce.Do(func() {
		v := os.Getenv("LAB_CONTEXT_KEY")
		if v == "" {
			v = "lab-context-key"
		}
		contextKey = []byte(v)
	})
	return contextKey
}

// SignContext подписывает имя пользователя ключом периметра.
func SignContext(user string) string {
	m := hmac.New(sha256.New, trustKey())
	m.Write([]byte(user))
	return hex.EncodeToString(m.Sum(nil))[:32]
}

// CheckContext отвечает на единственный вопрос сервиса: это имя проставил тот,
// у кого есть ключ, или тот, кто просто дотянулся до сети.
func CheckContext(user, sig string) bool {
	if user == "" || sig == "" {
		return false
	}
	return hmac.Equal([]byte(SignContext(user)), []byte(sig))
}
