package main

import (
	"fmt"
	"net/http"
	"time"
)

// Управление посредником, который портит сеть между службой и уведомлениями.
// Прокси создаётся сценой, а не руками: студент запускает одну команду.

const proxyName = "notifier"

// prepareProxy поднимает прокси и снимает с него все прежние поломки.
func (r *Run) prepareProxy() error {
	// Прокси мог остаться с прошлой сцены — пересоздаём, чтобы состояние не
	// протекало между прогонами.
	_ = r.deleteJSON(r.ToxiproxyURL + "/proxies/" + proxyName)

	err := r.postJSON(r.ToxiproxyURL+"/proxies", map[string]any{
		"name":     proxyName,
		"listen":   "0.0.0.0:8666",
		"upstream": "notifier:8070",
		"enabled":  true,
	}, nil)
	if err != nil {
		return fmt.Errorf("посредник не поднялся — поднят ли профиль net? %w", err)
	}
	// Прокси начинает слушать не мгновенно.
	time.Sleep(200 * time.Millisecond)
	return nil
}

// breakNetwork навешивает одну поломку. Каждая — отдельный вид отказа из тех,
// что курс учит различать по наблюдаемым признакам.
func (r *Run) breakNetwork(name, kind, stream string, attrs map[string]any) error {
	return r.postJSON(r.ToxiproxyURL+"/proxies/"+proxyName+"/toxics", map[string]any{
		"name":       name,
		"type":       kind,
		"stream":     stream,
		"toxicity":   1.0,
		"attributes": attrs,
	}, nil)
}

func (r *Run) healNetwork(name string) error {
	return r.deleteJSON(r.ToxiproxyURL + "/proxies/" + proxyName + "/toxics/" + name)
}

func (r *Run) deleteJSON(url string) error {
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	resp, err := r.ctl.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("%s ответил %d", url, resp.StatusCode)
	}
	return nil
}
