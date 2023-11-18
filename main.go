package main

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/raja-dettex/goatQ/daemon"
)

func main() {
	readCh := make(chan []byte)
	opts := daemon.Opts{ListenAddr: ":3000", TopicName: "test"}
	d := daemon.NewDaemon(opts)
	if err := d.Start(); err != nil {
		log.Fatal(err)
	}
	time.Sleep(time.Second * 3)
	go func() {
		for i := 0; i < 5; i++ {
			go sendWriteMessagetoSocket(fmt.Sprintf("WRITE hello%d", i))
		}
	}()

	//time.Sleep(time.Second * 3)

	go func(ch chan []byte) {
		for i := 0; i < 15; i++ {
			go sendReadMessagetoSocket(ch)
		}
	}(readCh)
	for data := range readCh {
		fmt.Println("fetched ", string(data))
	}
	select {}
}

func sendReadMessagetoSocket(readCh chan []byte) {
	conn, err := net.Dial("tcp", ":3000")
	if err != nil {
		fmt.Println(err)
	}
	_, err = conn.Write([]byte("READ "))
	if err != nil {
		fmt.Println(err)
	}
	buff := make([]byte, 1024)
	for {
		n, err := conn.Read(buff)
		if err != nil {
			fmt.Println("Read from the connection error ", err)
		}
		readCh <- buff[:n]
	}
}

func sendWriteMessagetoSocket(message string) {
	conn, err := net.Dial("tcp", ":3000")
	if err != nil {
		fmt.Println(err)
	}
	_, err = conn.Write([]byte(message))
	if err != nil {
		fmt.Println(err)
	}
	buff := make([]byte, 1024)
	for {
		n, err := conn.Read(buff)
		if err != nil {
			fmt.Println("write to the connection error ", err)
		}
		fmt.Println(string(buff[:n]))
	}
}
