package main

import (
	"fmt"
	_ "github.com/livekit/server-sdk-go/v2"
	_ "github.com/pion/webrtc/v4"
	_ "github.com/pion/webrtc/v4/pkg/media/h264reader"
	_ "github.com/pion/webrtc/v4/pkg/media/oggreader"
)

func main() {
	fmt.Println("Imports ok")
}
