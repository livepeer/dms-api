package misttriggers

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"net/url"
	"strconv"
	"strings"

	"bytes"
	"context"
	"time"

	"github.com/livepeer/go-tools/drivers"

	"github.com/livepeer/catalyst-api/cache"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/errors"
)
//{"video_H264_640x360_24fps_0":{"bframes":1,"bps":11205,"codec":"H264","firstms":541,"fpks":24000,"height":360,"idx":0,"init":"\u0001d\u0000\u001E\u00FF\u00E1\u0000 gd\u0000\u001E\u00AC,\u00A5\u0002\u0080\u00BF\u00E5\u00C0D\u0000\u0000\u000F\u00A0\u0000\u0002\u00EE\u0003\u0080\u0000\f5\u0000\u0006\u001A\u008B\u00BC\u00B8(\u0001\u0000\u0004h\u00EB\u008F,","jitter":200,"lastms":4958,"maxbps":11205,"trackid":256,"type":"video","width":640}}

type MistTrack struct {
// added by mist
	Id          int32  `json:"trackid"`
	ByteRate    int32  `json:"bps"`
	Kfps        int32  `json:"fpks"`
	Height      int32  `json:"height"`
	Width       int32  `json:"width"`
	Index       int32  `json:"idx"`
	Type        string `json:"type"`
	Codec       string `json:"codec"`
	StartTimeMs int32  `json:"firstms"`
	EndTimeMs   int32  `json:"lastms"`
// added by us
	manifestDestPath     string
}

type LiveTrackListTriggerJson = map[string]MistTrack

// create ByBitrate type which is a MistTrack slice
type ByBitrate []MistTrack

func (a ByBitrate) Len() int {
	return len(a)
}

func (a ByBitrate) Less(i, j int) bool {
	if a[i].ByteRate == a[j].ByteRate {
		// if two tracks have the same byterate, then sort by resolution
		return a[i].Width*a[i].Height < a[j].Width*a[j].Height
	} else {
		return a[i].ByteRate < a[j].ByteRate
	}
}

func (a ByBitrate) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func createPlaylist(multivariantPlaylist string, tracks []MistTrack) string {
	
	for i, _ := range tracks {
		multivariantPlaylist += fmt.Sprintf("#EXT-X-STREAM-INF:BANDWIDTH=%d,RESOLUTION=%dx%d\r\n%s\r\n", tracks[i].ByteRate*8, tracks[i].Width, tracks[i].Height, tracks[i].manifestDestPath)

	}	
	return multivariantPlaylist

}


func uploadPlaylist(uploadPath, manifest string) {

	log.Printf("YYY: storePlaylist %s %s", uploadPath, manifest)
	storageDriver, err := drivers.ParseOSURL(uploadPath, true)
	if err != nil {
		log.Printf("YYY: error drivers.ParseOSURL %v %s", err, uploadPath)
	}
	session := storageDriver.NewSession("")
	ctx := context.Background()
	_, err = session.SaveData(ctx, "", bytes.NewBuffer([]byte(manifest)), nil, 3*time.Second)
	if err != nil {
		log.Printf("YYY: error session.SaveData %v %s", err, uploadPath)
	}

}


// TriggerLiveTrackList responds to LIVE_TRACK_LIST trigger.
// It is stream-specific and must be blocking. The payload for this trigger is multiple lines,
// each separated by a single newline character (without an ending newline), containing data:
//
//	stream name
//	track list (JSON)
//
// TriggerLiveTrackList is used only by transcoding.
func (d *MistCallbackHandlersCollection) TriggerLiveTrackList(w http.ResponseWriter, req *http.Request, payload []byte) {
	lines := strings.Split(strings.TrimSuffix(string(payload), "\n"), "\n")
	streamName := lines[0]
	encodedTracks := lines[1]

	// Check that the name looks right for a stream we've completed as part of the Transcode workflow
	if !config.IsTranscodeStream(streamName) {
		errors.WriteHTTPBadRequest(w, "PUSH_END trigger invoked for something that isn't a transcode stream: "+streamName, nil)
		return
	}

	// Fetch the stream info from cache (cached when we kicked off the transcode process)
	info := cache.DefaultStreamCache.Transcoding.Get(streamName)
	if info == nil {
		errors.WriteHTTPInternalServerError(w, "LIVE_TRACK_LIST unknown push source: "+streamName, nil)
		return
	}

	// Check if LIVE_TRACK_LIST trigger is being fired *after* the push-from-Mist-to-S3 is complete
	var streamEnded = (encodedTracks == "null")
	if streamEnded {
		// SOURCE_PREFIX stream is no longer needed
		suffix := strings.TrimPrefix(streamName, config.RENDITION_PREFIX)
		inputStream := fmt.Sprintf("%s%s", config.SOURCE_PREFIX, suffix)
		if err := d.MistClient.DeleteStream(inputStream); err != nil {
			log.Printf("ERROR LIVE_TRACK_LIST DeleteStream(%s) %v", inputStream, err)
		}
		// Multiple pushes from RENDITION_PREFIX are in progress.
		return
	}

	var tracks LiveTrackListTriggerJson
	if err := json.Unmarshal([]byte(encodedTracks), &tracks); err != nil {
		errors.WriteHTTPInternalServerError(w, "LiveTrackListTriggerJson json decode error: "+streamName, err)
		return
	}
fmt.Printf("XXX: TRACKS: %v\n", tracks)

	multivariantPlaylist := "#EXTM3U\r\n"

	trackList := []MistTrack{} 

	// Build the full URL path that will be sent to Mist as the target upload location
	rootPathUrl, err := url.Parse(info.UploadDir)
	if err != nil {
		log.Fatal(err)
	}

	// upload each track (transcoded rendition) returned by Mist to S3
	for i := range tracks {
		// Only produce a rendition for each video track, selecting best audio track
		if tracks[i].Type != "video" {
			continue
		}

		dirPath := fmt.Sprintf("%s_%dx%d/stream.m3u8", streamName, tracks[i].Width, tracks[i].Height)
		dirPathUrl, err := url.JoinPath(info.UploadDir, dirPath)
		if err != nil {
			log.Fatal(err)
		}
	
		fullPathUrl, err := url.Parse(dirPathUrl)
		if err != nil {
			log.Fatal(err)
		}

		// Add URL query parameters (e.g. ?video=0&audio=maxbps) used by Mist to select
		// the correct trancoded rendtion track(s)
		urlParams := fullPathUrl.Query()
		urlParams.Add("video", strconv.FormatInt(int64(tracks[i].Index), 10))
		urlParams.Add("audio", "maxbps")
		fullPathUrl.RawQuery = urlParams.Encode()

		destination := fullPathUrl.String()

                if err := d.MistClient.PushStart(streamName, destination); err != nil {
                        log.Printf("> ERROR push to %s %v", destination, err)
                } else {
fmt.Println("XXX: STARTING PUSH AFTER LIVE_TRACK_LIST")
                        cache.DefaultStreamCache.Transcoding.AddDestination(streamName, destination)

			trackList = append(trackList, tracks[i])
			trackList[len(trackList)-1].manifestDestPath = dirPath
			fmt.Println("YYYA: trackList:", trackList)

//			profile, ok := info.GetMatchingProfile(tracks[i].Width, tracks[i].Height)
//			if !ok {
//				log.Printf("ERROR push doesn't match to any given profile %s", destination)
//			} else {
		//		multivariantPlaylist += fmt.Sprintf("#EXT-X-STREAM-INF:BANDWIDTH=%d,RESOLUTION=%dx%d\r\n%s\r\n", tracks[i].ByteRate*8, tracks[i].Width, tracks[i].Height, destination)
		//		log.Printf("YYY: multivariantPlaylist %s", multivariantPlaylist)

//			}

                }
	}

	// generate a sorted list:
	sort.Sort(sort.Reverse(ByBitrate(trackList)))
	fmt.Println("YYY: trackList:", trackList)
	manifest := createPlaylist(multivariantPlaylist, trackList)
	fmt.Println("YYY: manifest:", manifest)
	uploadPlaylist(fmt.Sprintf("%s/%s-master.m3u8", rootPathUrl.String(), streamName), manifest)
	

}
