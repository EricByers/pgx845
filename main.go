package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

const setupSQL = `drop table if exists j;

create table j(
	data jsonb NOT NULL
);

insert into j(data) values ('{ "phones":[ {"type": "mobile", "phone": "001001"} , {"type": "fix", "phone": "002002"} ] }');
`

type Foo struct {
	Phones []struct {
		Type  string `json:"type"`
		Phone string `json:"phone"`
	} `json:"phones"`
}

func main() {
	var memStats runtime.MemStats

	connConfig, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("pgxpool.ParseConfig unexpectedly failed: %v", err)
	}

	connConfig.MaxConns = 50
	pool, err := pgxpool.ConnectConfig(context.Background(), connConfig)
	if err != nil {
		log.Fatalf("pgxpool.ConnectConfig unexpectedly failed: %v", err)
	}
	defer pool.Close()

	func() {
		c, err := pool.Acquire(context.Background())
		if err != nil {
			log.Fatalf("pool.Acquire unexpectedly failed: %v", err)
		}
		defer c.Release()
		_, err = pool.Exec(context.Background(), setupSQL)
		if err != nil {
			log.Fatalln("Unable to setup database:", err)
		}
	}()

	conn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalln("Unable to connect to database:", err)
	}
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), setupSQL)
	if err != nil {
		log.Fatalln("Unable to setup database:", err)
	}

	for i := 0; i < 1000000000; i++ {
		var wg sync.WaitGroup

		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				row := pool.QueryRow(context.Background(), "select data from j;")
				var data Foo
				scanErr := row.Scan(&data)
				if scanErr != nil {
					log.Fatalln("rows.Scan unexpectedly failed:", scanErr)
				}
			}()
		}

		wg.Wait()

		if i%100000 == 0 {
			runtime.GC()
			runtime.ReadMemStats(&memStats)
			fmt.Printf("i=%d\tHeapAlloc=%d\tHeapObjects=%d\n", i, memStats.HeapAlloc, memStats.HeapObjects)
		}
	}
}
