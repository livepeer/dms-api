package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// invokeTriggerWorkaround fires LIVE_TRACK_LIST trigger as if Mist did
func invokeTriggerWorkaround(t *Transcoding) func() {
	return func() {
		for i := 0; i < 20; i++ {
			fmt.Printf("trigger not firing for produced stream %s\n", t.renditionsStream)
			req, err := http.NewRequest("POST", fmt.Sprintf("http://127.0.0.1:8080/json_%s.js", t.renditionsStream), nil)
			if err != nil {
				fmt.Printf("http.NewRequest error %v\n", err)
				return
			}
			client := &http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				fmt.Printf("client.Do error %v\n", err)
				return
			}
			defer resp.Body.Close()
			payload, err := io.ReadAll(resp.Body)
			if err != nil {
				fmt.Printf("io.ReadAll(resp.Body) error %v\n", err)
				return
			}
			response := string(payload)
			if resp.StatusCode != 200 {
				fmt.Printf("resp.StatusCode != 200 %v %v\n", resp.StatusCode, response)
				return
			}
			fmt.Printf("response: %v\n", response)
			meta := MetadataResponse{}
			err = json.Unmarshal(payload, &meta)
			if haveTracks := meta.Meta != nil; !haveTracks {
				fmt.Printf("> wait for stream info\n")
				time.Sleep(250 * time.Millisecond)
				continue
			}
			// construct trigger payload
			tracks := make(LiveTrackListTriggerJson)
			for index, info := range meta.Meta.Tracks {
				// key is unique per-track identifier so we can use index
				tracks[string(index)] = MistTrack{
					Type:        info.Type,
					Width:       info.Width,
					Height:      info.Height,
					Index:       int32(info.Idx),
					Kfps:        int32(info.Fpks),
					Codec:       info.Codec,
					StartTimeMs: int32(info.Firstms),
					EndTimeMs:   int32(info.Lastms),
				}
			}
			tracksJson, err := json.Marshal(tracks)
			if err != nil {
				fmt.Printf("json.Marshal(tracks) error %v\n", err)
				return
			}
			body := append([]byte(fmt.Sprintf("%s\n", t.renditionsStream)), tracksJson...)
			trigReq, err := http.NewRequest("POST", "http://127.0.0.1:4949/api/mist/trigger", bytes.NewBuffer(body))
			if err != nil {
				fmt.Printf("http.NewRequest(api/mist/trigger) error %v\n", err)
				return
			}
			trigReq.Header.Set("X-Trigger", "LIVE_TRACK_LIST")
			trigResp, err := client.Do(trigReq)
			if err != nil {
				fmt.Printf("client.Do(api/mist/trigger) error %v\n", err)
				return
			}
			defer trigResp.Body.Close()
			if trigResp.StatusCode != 200 {
				trigPayload, err := io.ReadAll(trigResp.Body)
				if err != nil {
					fmt.Printf("io.ReadAll(trigResp.Body) error %v\n", err)
					return
				}
				fmt.Printf("executed trigger LIVE_TRACK_LIST returns %d %s\n", trigResp.StatusCode, string(trigPayload))
				return
			}
			return
		}
	}
}

type MetadataTrackInfo struct {
	Bps      int    `json:"bps"`
	Channels int    `json:"channels"`
	Codec    string `json:"codec"`
	Firstms  int    `json:"firstms"`
	Fpks     int    `json:"fpks"`
	Width    int32  `json:"width"`
	Height   int32  `json:"height"`
	Idx      int    `json:"idx"`
	Init     string `json:"init"`
	Jitter   int    `json:"jitter"`
	Lastms   int    `json:"lastms"`
	Maxbps   int    `json:"maxbps"`
	Rate     int    `json:"rate"`
	Size     int    `json:"size"`
	Trackid  int    `json:"trackid"`
	Type     string `json:"type"`
}

type Metadata struct {
	Bframes      int `json:"bframes"`
	BufferWindow int `json:"buffer_window"`
	Jitter       int `json:"jitter"`
	Live         int `json:"live"`
	Maxkeepaway  int `json:"maxkeepaway"`
	Version      int `json:"version"`

	Tracks map[string]MetadataTrackInfo `json:"tracks"`
}

type MetadataResponse struct {
	Error      string    `json:"error"`
	Height     int       `json:"height"`
	Meta       *Metadata `json:"meta,omitempty"`
	Selver     int       `json:"selver"`
	Type       string    `json:"type"`
	Unixoffset int64     `json:"unixoffset"`
	Width      int       `json:"width"`
}
