// labctl — исполнитель сцен стенда. Запускается на время одной сцены,
// играет роль клиента, собирает наблюдения сервисов и печатает timeline.
// Наружу его зовёт `lab scene <id>` — студент про labctl не знает.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/kipyatl0/backend-architecture-lab/services/internal/lab"
)

// Драйверы сцен: реестр сцен — данные, поведение — здесь.
var drivers = map[string]func(context.Context, *Run) error{
	"scene01": scene01,
	"scene02": scene02,
	"scene03": scene03,
	"scene04": scene04,
	"scene05": scene05,
	"scene06": scene06,
	"scene07": scene07,
	"scene08": scene08,
	"scene09": scene09,
	"scene10": scene10,
	"scene11": scene11,
	"scene12": scene12,
	"scene13": scene13,
	"scene14": scene14,
	"scene15": scene15,
	"scene16": scene16,
	"scene17": scene17,
	"scene18": scene18,
	"scene19": scene19,
	"scene20": scene20,
	"scene21": scene21,
	"scene22": scene22,
	"scene23": scene23,
	"scene24": scene24,
	"scene25": scene25,
	"scene26": scene26,
	"scene27": scene27,
	"scene28": scene28,
	"scene29": scene29,
	"scene30": scene30,
	"scene31": scene31,
	"scene32": scene32,
	"scene33": scene33,
	"scene34": scene34,
	"scene35": scene35,
	"scene36": scene36,
	"scene37": scene37,
	"scene38": scene38,
	"scene41": scene41,
}

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}
	dir := lab.Env("LAB_SCENES_DIR", "/lab/scenes")

	var err error
	switch args[0] {
	case "scenes":
		err = cmdScenes(dir)
	case "scene":
		err = cmdScene(dir, args[1:])
	case "profile":
		// служебная команда для `lab`: какой профиль нужен сцене
		err = cmdProfile(dir, args[1:])
	case "state":
		err = cmdState()
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "ошибка: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "использование: labctl scenes | scene <id> [--explain] | state")
}

func cmdScenes(dir string) error {
	reg, err := loadRegistry(dir)
	if err != nil {
		return err
	}
	profile := os.Getenv("LAB_PROFILE")

	if profile == "" {
		fmt.Println("Сцены стенда (профиль не поднят — показаны все):")
	} else {
		fmt.Printf("Сцены профиля %s:\n", profile)
	}
	shown := 0
	for _, s := range reg.Scenes {
		if profile != "" && s.Profile != profile {
			continue
		}
		shown++
		det := "детерминирована"
		if !s.Deterministic {
			det = "НЕдетерминирована"
		}
		fmt.Printf("\n  %-4s %-9s %s\n", s.ID, s.Lesson, s.Title)
		fmt.Printf("       %s\n", s.Shows)
		fmt.Printf("       ~%d с · %s · ./lab scene %s\n", s.DurationS, det, s.ID)
	}
	if shown == 0 {
		fmt.Println("\n  (для этого профиля сцен пока нет)")
	}
	fmt.Println("\nПояснение к сцене: ./lab scene <id> --explain")
	return nil
}

func cmdScene(dir string, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("не указан номер сцены (например: ./lab scene 1)")
	}
	id := args[0]
	explain := false
	for _, a := range args[1:] {
		if a == "--explain" {
			explain = true
		}
	}

	reg, err := loadRegistry(dir)
	if err != nil {
		return err
	}
	scene, ok := reg.find(id)
	if !ok {
		return fmt.Errorf("сцены %s нет в реестре (список: ./lab scenes)", id)
	}
	script, err := loadScript(dir, scene)
	if err != nil {
		return err
	}
	driver, ok := drivers[scene.Driver]
	if !ok {
		return fmt.Errorf("для сцены %s не реализован драйвер %s", id, scene.Driver)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	run := newRun(scene, script)
	if err := driver(ctx, run); err != nil {
		return err
	}

	out, problems := run.report(explain)
	fmt.Print(out)

	if len(problems) > 0 {
		fmt.Fprintln(os.Stderr, "\n⚠ СЦЕНА РАЗОШЛАСЬ СО СЦЕНАРИЕМ")
		for _, p := range problems {
			fmt.Fprintln(os.Stderr, "  · "+p)
		}
		fmt.Fprintln(os.Stderr, "Такой вывод нельзя сверять с текстом шага. Обычная причина — "+
			"перегруженная машина.\nПрогони ещё раз: ./lab reset && ./lab scene "+id)
		os.Exit(1)
	}
	return nil
}

func cmdProfile(dir string, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("не указан номер сцены")
	}
	reg, err := loadRegistry(dir)
	if err != nil {
		return err
	}
	scene, ok := reg.find(args[0])
	if !ok {
		return fmt.Errorf("сцены %s нет в реестре (список: ./lab scenes)", args[0])
	}
	fmt.Println(scene.Profile)
	return nil
}

func cmdState() error {
	r := newRun(Scene{}, Script{})
	var state struct {
		Orders []struct {
			ID         int64  `json:"id"`
			Client     string `json:"client"`
			Restaurant string `json:"restaurant"`
			Amount     int64  `json:"amount"`
			Status     string `json:"status"`
		} `json:"orders"`
		Payments []struct {
			Order  int64  `json:"order"`
			Amount int64  `json:"amount"`
			Status string `json:"status"`
			Charge string `json:"charge"`
		} `json:"payments"`
		Assignments []struct {
			Order int64  `json:"order"`
			Text  string `json:"text"`
		} `json:"assignments"`
		Notifications []struct {
			Order int64  `json:"order"`
			Text  string `json:"text"`
		} `json:"notifications"`
		Totals struct {
			Orders       int   `json:"orders"`
			Charged      int   `json:"charged"`
			ChargedTotal int64 `json:"charged_total"`
		} `json:"totals"`
	}
	if err := r.getJSON(r.MonolithURL+"/_lab/state", &state); err != nil {
		return fmt.Errorf("служба не отвечает — поднят ли профиль? (./lab up mono): %w", err)
	}

	fmt.Printf("Заказы (%d)\n", len(state.Orders))
	for _, o := range state.Orders {
		fmt.Printf("  %d  %-8s %5d  %s · %s\n", o.ID, o.Status, o.Amount, o.Client, o.Restaurant)
	}
	fmt.Printf("Платежи (%d)\n", len(state.Payments))
	for _, p := range state.Payments {
		fmt.Printf("  %d  %-8s %5d  %s\n", p.Order, p.Status, p.Amount, p.Charge)
	}
	fmt.Printf("Курьеры (%d)\n", len(state.Assignments))
	for _, a := range state.Assignments {
		fmt.Printf("  %d  %s\n", a.Order, a.Text)
	}
	fmt.Printf("Уведомления (%d)\n", len(state.Notifications))
	for _, n := range state.Notifications {
		fmt.Printf("  %d  %s\n", n.Order, n.Text)
	}
	fmt.Printf("\nИтого: заказов %d, списаний %d на %d\n",
		state.Totals.Orders, state.Totals.Charged, state.Totals.ChargedTotal)
	if len(state.Orders) > 0 {
		fmt.Println("Это состояние осталось от последней сцены. Сцена сбрасывает данные сама;")
		fmt.Println("./lab reset пересоздаёт тома целиком, если стенд надо вернуть к чистому листу.")
	}
	return nil
}
