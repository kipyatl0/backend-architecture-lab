package lab

// Обмен сообщениями — общая часть для всех сервисов стенда. Две модели стоят
// рядом намеренно: курс показывает их на одном и том же потоке событий, и
// разница между «сообщение забрали — его больше нет» и «сообщение осталось,
// у каждого читателя свой курсор» видна только при таком сравнении.
//
// Никакой логики предметной области здесь нет: сервис решает, что публиковать
// и как обрабатывать, а этот файл знает только про доставку.

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ── имена, которые видит студент ────────────────────────────────────────────
//
// Всё, что печатается в timeline или встречается в тексте шага, живёт здесь и
// только здесь: имена очередей и тем — часть контракта с текстом курса
// (docs/CANON.md), а не деталь реализации.

const (
	Exchange   = "orders"        // обменник: одно событие — сколько угодно очередей
	QueueCour  = "courier"       // очередь назначения курьеров
	QueueNotif = "notifier"      // очередь уведомлений
	DeadX      = "orders.dead"   // обменник разбора неудач
	QueueDead  = "courier.dead"  // куда уходит то, что не удалось обработать
	QueueManu  = "manual-review" // очередь ручного разбора: компенсация не прошла
	Topic      = "order-events"  // лог событий заказа
	TopicParts = 3               // партиций у лога: порядок живёт внутри партиции
)

// Msg — событие заказа. Факт в прошедшем времени: отправитель не знает, кто
// на него подпишется, и ничего не ждёт в ответ.
type Msg struct {
	Type    string `json:"type"`  // order.paid, order.assigned, order.cancelled
	Order   int64  `json:"order"` // он же ключ партиционирования
	Amount  int64  `json:"amount,omitempty"`
	Client  string `json:"client,omitempty"`
	Courier string `json:"courier,omitempty"`
	Reason  string `json:"reason,omitempty"`
	// Порядковый номер события в пределах заказа: по нему потребитель узнаёт,
	// что события пришли не в том порядке, в каком случились.
	Seq int `json:"seq,omitempty"`
	// Сколько занимает обработка именно этого события. Не украшение сцены:
	// события одного заказа требуют разной работы всегда, и порядок ломается
	// не от «гонки вообще», а от того, что короткое обгоняет длинное.
	WorkMS int `json:"work_ms,omitempty"`
	// Идентификатор сообщения — то, по чему дедуплицирует идемпотентный
	// потребитель. У повторной доставки он тот же самый; у повторной
	// публикации (исходящий ящик отправил дважды) — тоже.
	ID string `json:"id"`
}

func (e Msg) Key() string { return strconv.FormatInt(e.Order, 10) }

func (e Msg) Bytes() []byte {
	b, _ := json.Marshal(e)
	return b
}

func ParseMsg(raw []byte) (Msg, error) {
	var e Msg
	err := json.Unmarshal(raw, &e)
	return e, err
}

// ── модель очереди: RabbitMQ ────────────────────────────────────────────────

type AMQP struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

func DialAMQP(url string) (*AMQP, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("очередь недоступна: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("канал не открыт: %w", err)
	}
	return &AMQP{conn: conn, ch: ch}, nil
}

// DialAMQPWait ждёт брокер: сервис поднимается вместе с ним, и первые секунды
// подключение штатно не удаётся. Отдельный сервис здесь падать не должен —
// иначе стенд превращается в гонку за порядок старта.
func DialAMQPWait(ctx context.Context, url string, wait time.Duration) (*AMQP, error) {
	deadline := time.Now().Add(wait)
	for {
		a, err := DialAMQP(url)
		if err == nil {
			return a, nil
		}
		if time.Now().After(deadline) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (a *AMQP) Close() {
	if a == nil {
		return
	}
	if a.ch != nil {
		a.ch.Close()
	}
	if a.conn != nil {
		a.conn.Close()
	}
}

func (a *AMQP) Channel() *amqp.Channel { return a.ch }

// Topology — что именно сцена хочет видеть в брокере. Пересоздаётся каждой
// сценой целиком: стенд не копит сообщения между прогонами, иначе вывод
// зависел бы от того, что студент запускал до этого.
type Topology struct {
	// Предел повторных доставок. 0 — не задавать вовсе (RabbitMQ 4.x сам
	// поставит 20), -1 — снять предел совсем, положительное число — задать.
	// Снимать предел приходится явно: с версии 4.0 брокер защищается от
	// бесконечного повтора по умолчанию, и «как было раньше» надо просить.
	DeliveryLimit int
	// Заводить ли очередь ручного разбора (сцена 21).
	ManualReview bool
}

// Declare сносит прежнюю топологию и объявляет её заново. Очереди — кворумные:
// предел повторных доставок есть только у них, а без него сцену «сообщение
// ушло в очередь разбора» пришлось бы играть руками потребителя и она перестала
// бы показывать механику брокера.
func (a *AMQP) Declare(t Topology) error {
	for _, q := range []string{QueueCour, QueueNotif, QueueDead, QueueManu} {
		if _, err := a.ch.QueueDelete(q, false, false, false); err != nil {
			// Канал после ошибки непригоден — переоткрываем и идём дальше:
			// «очереди не было» здесь не ошибка, а нормальный первый запуск.
			a.ch.Close()
			ch, cerr := a.conn.Channel()
			if cerr != nil {
				return cerr
			}
			a.ch = ch
		}
	}
	for _, x := range []string{Exchange, DeadX} {
		if err := a.ch.ExchangeDelete(x, false, false); err != nil {
			return err
		}
	}

	if err := a.ch.ExchangeDeclare(Exchange, "topic", true, false, false, false, nil); err != nil {
		return err
	}
	if err := a.ch.ExchangeDeclare(DeadX, "fanout", true, false, false, false, nil); err != nil {
		return err
	}

	args := amqp.Table{
		"x-queue-type":           "quorum",
		"x-dead-letter-exchange": DeadX,
	}
	if t.DeliveryLimit != 0 {
		args["x-delivery-limit"] = int32(t.DeliveryLimit)
	}
	if _, err := a.ch.QueueDeclare(QueueCour, true, false, false, false, args); err != nil {
		return err
	}
	if _, err := a.ch.QueueDeclare(QueueNotif, true, false, false, false,
		amqp.Table{"x-queue-type": "quorum"}); err != nil {
		return err
	}
	if _, err := a.ch.QueueDeclare(QueueDead, true, false, false, false,
		amqp.Table{"x-queue-type": "quorum"}); err != nil {
		return err
	}
	if t.ManualReview {
		if _, err := a.ch.QueueDeclare(QueueManu, true, false, false, false,
			amqp.Table{"x-queue-type": "quorum"}); err != nil {
			return err
		}
	}

	// Обе очереди слушают один и тот же обменник по одному и тому же ключу:
	// это и есть веерная рассылка — отправитель по-прежнему не знает, сколько
	// у него подписчиков.
	for _, q := range []string{QueueCour, QueueNotif} {
		if err := a.ch.QueueBind(q, "order.#", Exchange, false, nil); err != nil {
			return err
		}
	}
	return a.ch.QueueBind(QueueDead, "", DeadX, false, nil)
}

func (a *AMQP) Publish(ctx context.Context, key string, e Msg) error {
	return a.ch.PublishWithContext(ctx, Exchange, key, false, false, amqp.Publishing{
		ContentType:  "application/json",
		MessageId:    e.ID,
		DeliveryMode: amqp.Persistent,
		Body:         e.Bytes(),
	})
}

// PublishTo кладёт сообщение прямо в очередь, минуя обменник. Нужно ровно
// одному сценарию — отправке в очередь ручного разбора.
func (a *AMQP) PublishTo(ctx context.Context, queue string, e Msg) error {
	return a.ch.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
		ContentType:  "application/json",
		MessageId:    e.ID,
		DeliveryMode: amqp.Persistent,
		Body:         e.Bytes(),
	})
}

// Consume выдаёт поток доставок. Подтверждение всегда ручное: момент
// подтверждения — предмет урока, и прятать его за автоматикой нельзя.
func (a *AMQP) Consume(queue, tag string, prefetch int) (<-chan amqp.Delivery, error) {
	if err := a.ch.Qos(prefetch, 0, false); err != nil {
		return nil, err
	}
	return a.ch.Consume(queue, tag, false, false, false, false, nil)
}

// Depth — сколько сообщений сейчас лежит в очереди. Пассивное объявление не
// меняет очередь и служит единственным честным способом её посчитать.
func (a *AMQP) Depth(queue string) (int, error) {
	q, err := a.ch.QueueDeclarePassive(queue, true, false, false, false,
		amqp.Table{"x-queue-type": "quorum"})
	if err != nil {
		return 0, err
	}
	return q.Messages, nil
}

// Drain забирает всё, что лежит в очереди, ничего не оставляя. Так сцена
// показывает содержимое очереди разбора и так же проверяет, что очередь пуста.
func (a *AMQP) Drain(queue string) ([]Msg, error) {
	var out []Msg
	for {
		d, ok, err := a.ch.Get(queue, true)
		if err != nil {
			return out, err
		}
		if !ok {
			return out, nil
		}
		e, err := ParseMsg(d.Body)
		if err != nil {
			// Отравленное сообщение до сюда доходит как есть: разобрать его
			// нельзя, но факт его наличия — и есть результат сцены.
			out = append(out, Msg{Type: "неразбираемое", ID: d.MessageId})
			continue
		}
		out = append(out, e)
	}
}

// ── модель лога: Kafka ──────────────────────────────────────────────────────

type Kafka struct {
	cl *kgo.Client
}

// DialKafka — клиент только для отправки. Читателя заводит DialKafkaGroup:
// у читателя есть группа и курсор, и это не деталь настройки, а вся разница
// между двумя моделями.
func DialKafka(brokers []string) (*Kafka, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, err
	}
	return &Kafka{cl: cl}, nil
}

// DialKafkaGroup — читатель в группе. Смещение не двигается само: когда
// потребитель отмечает сообщение обработанным — предмет урока про гарантии.
func DialKafkaGroup(brokers []string, group string, fromStart bool) (*Kafka, error) {
	offset := kgo.NewOffset().AtEnd()
	if fromStart {
		offset = kgo.NewOffset().AtStart()
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(Topic),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(offset),
	)
	if err != nil {
		return nil, err
	}
	return &Kafka{cl: cl}, nil
}

func (k *Kafka) Close() {
	if k != nil && k.cl != nil {
		k.cl.Close()
	}
}

func (k *Kafka) Client() *kgo.Client { return k.cl }

// WaitKafka ждёт брокер по той же причине, что и очередь: порядок старта
// контейнеров не должен решать, работает ли стенд.
func WaitKafka(ctx context.Context, brokers []string, wait time.Duration) error {
	deadline := time.Now().Add(wait)
	for {
		k, err := DialKafka(brokers)
		if err == nil {
			pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			err = k.cl.Ping(pingCtx)
			cancel()
			k.Close()
			if err == nil {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("лог недоступен: %w", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (k *Kafka) Produce(ctx context.Context, e Msg) error {
	return k.cl.ProduceSync(ctx, &kgo.Record{
		Topic: Topic,
		Key:   []byte(e.Key()),
		Value: e.Bytes(),
	}).FirstErr()
}

// ProduceTo кладёт запись в названную партицию. Ключ при этом не трогается:
// сцене про порядок нужно, чтобы все события заказа гарантированно легли в
// одну партицию, а не «скорее всего легли».
func (k *Kafka) ProduceTo(ctx context.Context, partition int32, e Msg) error {
	return k.cl.ProduceSync(ctx, &kgo.Record{
		Topic:     Topic,
		Key:       []byte(e.Key()),
		Value:     e.Bytes(),
		Partition: partition,
	}).FirstErr()
}

// RecreateTopic сносит лог и заводит его заново. Сцена обязана начинаться с
// пустого лога: студент сверяет число записей с текстом шага.
func RecreateTopic(ctx context.Context, brokers []string, partitions int32) error {
	cl, err := DialKafka(brokers)
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl.cl)

	if _, err := adm.DeleteTopics(ctx, Topic); err != nil {
		return err
	}
	// Удаление в KRaft асинхронное: создать тему сразу после удаления —
	// значит иногда получить прежнее число партиций и молча сломать сцену 16.
	deadline := time.Now().Add(30 * time.Second)
	for {
		topics, err := adm.ListTopics(ctx, Topic)
		if err != nil {
			return err
		}
		if !topics.Has(Topic) {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("прежняя тема %s не удалилась за 30 с", Topic)
		}
		time.Sleep(200 * time.Millisecond)
	}

	for {
		resp, err := adm.CreateTopics(ctx, partitions, 1, nil, Topic)
		if err != nil {
			return err
		}
		created := resp[Topic]
		if created.Err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("тема %s не создана: %w", Topic, created.Err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// EndOffsets — сколько записей лежит в логе. Для студента это ответ на вопрос
// «а сообщение-то ещё там?», ради которого модель лога и вводится.
func EndOffsets(ctx context.Context, brokers []string) (int64, error) {
	cl, err := DialKafka(brokers)
	if err != nil {
		return 0, err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl.cl)
	offsets, err := adm.ListEndOffsets(ctx, Topic)
	if err != nil {
		return 0, err
	}
	var total int64
	offsets.Each(func(o kadm.ListedOffset) { total += o.Offset })
	return total, nil
}

// ReadAll читает лог новой группой с начала и возвращает всё, что там лежит.
// Именно этим сцена 13 показывает, что сообщение никуда не делось.
func ReadAll(ctx context.Context, brokers []string, group string, idle time.Duration) ([]Msg, error) {
	k, err := DialKafkaGroup(brokers, group, true)
	if err != nil {
		return nil, err
	}
	defer k.Close()

	var out []Msg
	for {
		pollCtx, cancel := context.WithTimeout(ctx, idle)
		fetches := k.cl.PollFetches(pollCtx)
		cancel()
		if fetches.IsClientClosed() {
			return out, nil
		}
		empty := true
		fetches.EachRecord(func(r *kgo.Record) {
			empty = false
			if e, err := ParseMsg(r.Value); err == nil {
				out = append(out, e)
			}
		})
		// Пустая выборка означает, что читать больше нечего: лог кончился.
		if empty {
			return out, nil
		}
	}
}
