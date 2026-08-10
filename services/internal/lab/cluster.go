package lab

// Кластер из трёх узлов (профиль cluster, m08). Всё, что здесь есть, отвечает
// на три вопроса модуля: сколько копий у записи, кто из них в синхронном
// наборе и кого выберут лидером, когда прежний умер.
//
// Отдельный файл, а не дописанный bus.go: там модель обмена, здесь — модель
// хранения одной и той же записи на нескольких узлах.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// PartState — то, что студент читает в выводе сцены: где лежат копии партиции,
// кто из них лидер и кто числится синхронным.
type PartState struct {
	Partition int32
	Leader    int32
	Replicas  []int32
	ISR       []int32
}

func adminClient(brokers []string) (*kgo.Client, *kadm.Client, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		// Узлы в этих сценах умирают по сценарию: клиент, который ждёт
		// упавший узел по полминуты, сделал бы сцену нечитаемой.
		kgo.RetryTimeout(10*time.Second),
		kgo.RequestTimeoutOverhead(5*time.Second),
	)
	if err != nil {
		return nil, nil, err
	}
	return cl, kadm.NewClient(cl), nil
}

// TopicState — состояние копий темы прямо сейчас.
func TopicState(ctx context.Context, brokers []string, topic string) ([]PartState, error) {
	cl, adm, err := adminClient(brokers)
	if err != nil {
		return nil, err
	}
	defer cl.Close()

	md, err := adm.Metadata(ctx, topic)
	if err != nil {
		return nil, err
	}
	det, ok := md.Topics[topic]
	if !ok || det.Err != nil {
		return nil, fmt.Errorf("темы %s нет в кластере", topic)
	}
	var out []PartState
	for _, p := range det.Partitions.Sorted() {
		out = append(out, PartState{
			Partition: p.Partition,
			Leader:    p.Leader,
			Replicas:  p.Replicas,
			ISR:       p.ISR,
		})
	}
	return out, nil
}

// BrokerHosts — какой номер узла каким именем зовётся в сети стенда. Сцена
// печатает имена (kafka-2), а брокер знает номера — перевод нужен обеим
// сторонам: и выводу, и остановке контейнера.
func BrokerHosts(ctx context.Context, brokers []string) (map[int32]string, error) {
	cl, adm, err := adminClient(brokers)
	if err != nil {
		return nil, err
	}
	defer cl.Close()

	bs, err := adm.ListBrokers(ctx)
	if err != nil {
		return nil, err
	}
	out := map[int32]string{}
	for _, b := range bs {
		out[b.NodeID] = b.Host
	}
	return out, nil
}

// RecreateTopicRF пересоздаёт тему с нужным числом копий и настройками.
// Как и в m06: каждая сцена начинается с чистого лога, иначе студент сверяет
// число записей с чужими.
func RecreateTopicRF(ctx context.Context, brokers []string, topic string,
	partitions int32, rf int16, cfg map[string]string) error {
	cl, adm, err := adminClient(brokers)
	if err != nil {
		return err
	}
	defer cl.Close()

	deadline := time.Now().Add(45 * time.Second)
	if err := dropTopic(ctx, adm, topic, deadline); err != nil {
		return err
	}
	conf := map[string]*string{}
	for k, v := range cfg {
		val := v
		conf[k] = &val
	}
	for {
		resp, err := adm.CreateTopics(ctx, partitions, rf, conf, topic)
		if err != nil {
			return err
		}
		if created := resp[topic]; created.Err == nil {
			return nil
		} else if time.Now().After(deadline) {
			return fmt.Errorf("тема %s не создана: %w", topic, created.Err)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// PlaceReplicas раскладывает копии партиции по названным узлам и делает
// первый из них лидером. Без этого кластер раскладывает их сам и каждый раз
// по-разному — а сцена печатает имена узлов, и студент сверяет вывод буква в
// букву. Предмет сцены от того, какой именно узел лидер, не зависит.
func PlaceReplicas(ctx context.Context, brokers []string, topic string,
	part int32, replicas []int32) error {
	cl, adm, err := adminClient(brokers)
	if err != nil {
		return err
	}
	defer cl.Close()

	var req kadm.AlterPartitionAssignmentsReq
	req.Assign(topic, part, replicas)
	resp, err := adm.AlterPartitionAssignments(ctx, req)
	if err != nil {
		return err
	}
	if err := resp.Error(); err != nil {
		return fmt.Errorf("копии не разложены по узлам: %w", err)
	}

	same := func(a, b []int32) bool {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}
	// Ждём, пока перекладка закончится, и только потом просим кластер
	// поставить лидером первую копию: до конца перекладки он откажет.
	if _, err := WaitLeader(ctx, brokers, topic, part, func(p PartState) bool {
		return same(p.Replicas, replicas) && len(p.ISR) == len(replicas)
	}, 60*time.Second); err != nil {
		return err
	}
	set := kadm.TopicsSet{}
	set.Add(topic, part)
	if _, err := adm.ElectLeaders(ctx, kadm.ElectPreferredReplica, set); err != nil {
		return err
	}
	_, err = WaitLeader(ctx, brokers, topic, part, func(p PartState) bool {
		return p.Leader == replicas[0]
	}, 60*time.Second)
	return err
}

// SetTopicConfig меняет настройку темы на живом кластере: сцена 24 разрешает
// небезопасный выбор лидера ровно в тот момент, когда объясняет его цену.
func SetTopicConfig(ctx context.Context, brokers []string, topic string, cfg map[string]string) error {
	cl, adm, err := adminClient(brokers)
	if err != nil {
		return err
	}
	defer cl.Close()

	var alters []kadm.AlterConfig
	for k, v := range cfg {
		val := v
		alters = append(alters, kadm.AlterConfig{Op: kadm.SetConfig, Name: k, Value: &val})
	}
	resps, err := adm.AlterTopicConfigs(ctx, alters, topic)
	if err != nil {
		return err
	}
	for _, r := range resps {
		if r.Err != nil {
			return fmt.Errorf("настройка темы %s не применена: %w", topic, r.Err)
		}
	}
	return nil
}

// Writer — отправитель с явно названным требованием к подтверждению. Ровно эта
// ручка и есть предмет m08 l02: «сколько копий обязано подтвердить запись».
type Writer struct {
	cl *kgo.Client
}

// NewWriter: acks = -1 (все синхронные копии), 1 (только лидер), 0 (никто).
// Повторы ограничены намеренно — отправитель, который вечно перепосылает,
// показал бы студенту не отказ, а зависание.
func NewWriter(brokers []string, acks int) (*Writer, error) {
	req := kgo.AllISRAcks()
	idem := false
	switch acks {
	case 0:
		req = kgo.NoAck()
	case 1:
		req = kgo.LeaderAck()
	default:
		idem = true
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(req),
		kgo.RecordRetries(2),
		kgo.RecordDeliveryTimeout(8 * time.Second),
		kgo.ProduceRequestTimeout(3 * time.Second),
		kgo.RetryTimeout(8 * time.Second),
	}
	if !idem {
		opts = append(opts, kgo.DisableIdempotentWrite())
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &Writer{cl: cl}, nil
}

func (w *Writer) Close() { w.cl.Close() }

// Warm — соединиться и забрать метаданные до начала сцены. Первая отправка
// иначе стоит секунду на знакомство с кластером, и эта секунда попадает в
// timeline как задержка записи, которой не было.
func (w *Writer) Warm(ctx context.Context) error {
	w.cl.ForceMetadataRefresh()
	return w.cl.Ping(ctx)
}

// Write кладёт событие в названную партицию и ждёт ответа кластера. Ошибка
// здесь — не сбой стенда, а наблюдение сцены.
func (w *Writer) Write(ctx context.Context, topic string, part int32, e Msg) error {
	return w.cl.ProduceSync(ctx, &kgo.Record{
		Topic:     topic,
		Partition: part,
		Key:       []byte(e.Key()),
		Value:     e.Bytes(),
	}).FirstErr()
}

// WriteAt — то же, но с временем, которое ставит отправитель. Нужно там, где
// предмет сцены — часы записавшего узла (m08 l04).
func (w *Writer) WriteAt(ctx context.Context, topic string, part int32, e Msg, ts time.Time) error {
	return w.cl.ProduceSync(ctx, &kgo.Record{
		Topic:     topic,
		Partition: part,
		Key:       []byte(e.Key()),
		Value:     e.Bytes(),
		Timestamp: ts,
	}).FirstErr()
}

// ErrName — короткое имя отказа кластера (NOT_ENOUGH_REPLICAS и подобные).
// В timeline печатается именно оно: полное описание занимает три строки и
// ничего не добавляет к тому, что случилось.
func ErrName(err error) string {
	if err == nil {
		return ""
	}
	var ke *kerr.Error
	if errors.As(err, &ke) {
		return ke.Message
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "таймаут отправки"
	}
	return err.Error()
}

// ElectUnclean — выбрать лидером живую копию, даже если она не в синхронном
// наборе. Команда оператора, а не решение кластера: цена решения — записи,
// которых у этой копии нет.
func ElectUnclean(ctx context.Context, brokers []string, topic string, part int32) error {
	cl, adm, err := adminClient(brokers)
	if err != nil {
		return err
	}
	defer cl.Close()

	set := kadm.TopicsSet{}
	set.Add(topic, part)
	res, err := adm.ElectLeaders(ctx, kadm.ElectLiveReplica, set)
	if err != nil {
		return err
	}
	for _, parts := range res {
		for _, r := range parts {
			if r.Err != nil {
				return fmt.Errorf("выбор лидера не удался: %w", r.Err)
			}
		}
	}
	return nil
}

// WaitISR ждёт, пока синхронный набор партиции станет нужного размера. Ждать
// по состоянию, а не по часам: скорость выхода узла из набора зависит от
// машины, а сам факт — нет.
func WaitISR(ctx context.Context, brokers []string, topic string, part int32,
	want int, limit time.Duration) (PartState, error) {
	deadline := time.Now().Add(limit)
	var last PartState
	for {
		states, err := TopicState(ctx, brokers, topic)
		if err == nil {
			for _, s := range states {
				if s.Partition != part {
					continue
				}
				last = s
				if len(s.ISR) == want {
					return s, nil
				}
			}
		}
		if time.Now().After(deadline) {
			return last, fmt.Errorf("синхронных копий у партиции %d осталось %d, а сцена ждала %d",
				part, len(last.ISR), want)
		}
		select {
		case <-ctx.Done():
			return last, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// WaitLeader ждёт появления лидера у партиции (и его смены). -1 означает
// «лидера нет» — именно это видно, пока кластер не выбрал нового.
func WaitLeader(ctx context.Context, brokers []string, topic string, part int32,
	accept func(PartState) bool, limit time.Duration) (PartState, error) {
	deadline := time.Now().Add(limit)
	var last PartState
	for {
		states, err := TopicState(ctx, brokers, topic)
		if err == nil {
			for _, s := range states {
				if s.Partition != part {
					continue
				}
				last = s
				if accept(s) {
					return s, nil
				}
			}
		}
		if time.Now().After(deadline) {
			return last, fmt.Errorf("партиция %d не пришла в ожидаемое состояние за %s", part, limit)
		}
		select {
		case <-ctx.Done():
			return last, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// CountRecords — сколько записей лежит в названной теме. Ответ на вопрос
// «а запись-то на месте?» после смены лидера.
func CountRecords(ctx context.Context, brokers []string, topic string) (int64, error) {
	cl, adm, err := adminClient(brokers)
	if err != nil {
		return 0, err
	}
	defer cl.Close()

	offsets, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return 0, err
	}
	var total int64
	offsets.Each(func(o kadm.ListedOffset) { total += o.Offset })
	return total, nil
}
