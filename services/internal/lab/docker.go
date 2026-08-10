package lab

// Управление контейнерами из сцены. До m08 стенду это было не нужно: всё, что
// сцена ломала, ломалось изнутри процесса — у приложения есть ручка «умри».
// У брокера и у базы такой ручки нет, а предмет модуля — ровно смерть узла:
// кворум, выбор лидера, отставшая копия. Поэтому исполнитель сцен получает
// доступ к сокету Docker и умеет останавливать и поднимать узлы кластера.
//
// Клиент нарочно крошечный: список контейнеров проекта, stop, kill, start и
// ожидание здоровья. Полноценная библиотека тянула бы за собой пол-Docker ради
// пяти запросов.

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const dockerAPI = "http://docker/v1.45"

type Docker struct {
	http    *http.Client
	project string
}

func NewDocker(project string) *Docker {
	sock := Env("LAB_DOCKER_SOCKET", "/var/run/docker.sock")
	tr := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		},
	}
	return &Docker{
		http:    &http.Client{Transport: tr, Timeout: 60 * time.Second},
		project: project,
	}
}

func (d *Docker) do(ctx context.Context, method, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, dockerAPI+path, nil)
	if err != nil {
		return err
	}
	resp, err := d.http.Do(req)
	if err != nil {
		return fmt.Errorf("docker: %w (доступен ли сокет /var/run/docker.sock?)", err)
	}
	defer resp.Body.Close()
	// 304 — «уже в этом состоянии»: остановленный контейнер остановить нельзя,
	// и для сцены это не ошибка.
	if resp.StatusCode == http.StatusNotModified {
		return nil
	}
	if resp.StatusCode >= 300 {
		var e struct {
			Message string `json:"message"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&e)
		return fmt.Errorf("docker %s %s: %d %s", method, path, resp.StatusCode, e.Message)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

// ID контейнера по имени сервиса Compose. Имя контейнера собирается из имени
// проекта и номера копии — искать по меткам надёжнее, чем по имени.
func (d *Docker) id(ctx context.Context, service string) (string, error) {
	filters := fmt.Sprintf(`{"label":["com.docker.compose.project=%s","com.docker.compose.service=%s"]}`,
		d.project, service)
	var list []struct {
		ID    string `json:"Id"`
		State string `json:"State"`
	}
	path := "/containers/json?all=1&filters=" + url.QueryEscape(filters)
	if err := d.do(ctx, http.MethodGet, path, &list); err != nil {
		return "", err
	}
	if len(list) == 0 {
		return "", fmt.Errorf("контейнер сервиса %s не найден — поднят ли профиль?", service)
	}
	return list[0].ID, nil
}

// Stop — корректная остановка: узел успевает передать лидерство и попрощаться
// с кворумом. Так уходит узел, которого выключили штатно.
func (d *Docker) Stop(ctx context.Context, service string) error {
	id, err := d.id(ctx, service)
	if err != nil {
		return err
	}
	return d.do(ctx, http.MethodPost, "/containers/"+id+"/stop?t=20", nil)
}

// Kill — смерть без прощания. Разница со Stop не косметическая: узел, успевший
// передать лидерство, не оставляет после себя ни отставшей копии, ни выбора
// между доступностью и целостностью.
func (d *Docker) Kill(ctx context.Context, service string) error {
	id, err := d.id(ctx, service)
	if err != nil {
		return err
	}
	return d.do(ctx, http.MethodPost, "/containers/"+id+"/kill?signal=SIGKILL", nil)
}

func (d *Docker) Start(ctx context.Context, service string) error {
	id, err := d.id(ctx, service)
	if err != nil {
		return err
	}
	return d.do(ctx, http.MethodPost, "/containers/"+id+"/start", nil)
}

// Restart нужен там, где узел должен собраться заново: реплика держит данные в
// памяти, и перезапуск контейнера — это и есть «снять базовую копию заново».
func (d *Docker) Restart(ctx context.Context, service string) error {
	id, err := d.id(ctx, service)
	if err != nil {
		return err
	}
	return d.do(ctx, http.MethodPost, "/containers/"+id+"/restart?t=20", nil)
}

type containerState struct {
	State struct {
		Running bool `json:"Running"`
		Health  struct {
			Status string `json:"Status"`
		} `json:"Health"`
	} `json:"State"`
}

func (d *Docker) state(ctx context.Context, service string) (containerState, error) {
	var st containerState
	id, err := d.id(ctx, service)
	if err != nil {
		return st, err
	}
	err = d.do(ctx, http.MethodGet, "/containers/"+id+"/json", &st)
	return st, err
}

func (d *Docker) Running(ctx context.Context, service string) (bool, error) {
	st, err := d.state(ctx, service)
	return st.State.Running, err
}

// WaitHealthy ждёт не запуска процесса, а готовности узла обслуживать: у
// брокера между ними десяток секунд, и сцена, начавшая раньше, врёт.
func (d *Docker) WaitHealthy(ctx context.Context, service string, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	for {
		st, err := d.state(ctx, service)
		if err == nil && st.State.Running {
			s := strings.ToLower(st.State.Health.Status)
			if s == "healthy" || s == "" {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("узел %s не стал здоровым за %s", service, limit)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// WaitStopped — обратная сторона: сцена не должна двигаться дальше, пока узел
// ещё жив, иначе замер «когда именно встала запись» окажется про другое.
func (d *Docker) WaitStopped(ctx context.Context, service string, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	for {
		running, err := d.Running(ctx, service)
		if err == nil && !running {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("узел %s не остановился за %s", service, limit)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
}
