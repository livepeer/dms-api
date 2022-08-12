package handlers

import (
	"bytes"
	"github.com/julienschmidt/httprouter"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestOKHandler(t *testing.T) {
	require := require.New(t)

	catalystApiHandlers := CatalystAPIHandlersCollection{}
	router := httprouter.New()
	req, _ := http.NewRequest("GET", "/ok", nil)
	rr := httptest.NewRecorder()
	router.GET("/ok", catalystApiHandlers.Ok())
	router.ServeHTTP(rr, req)

	require.Equal(rr.Body.String(), "OK")
}

func TestSuccessfulVODUploadHandler(t *testing.T) {
	require := require.New(t)

	catalystApiHandlers := CatalystAPIHandlersCollection{MistClient: StubMistClient{}, StreamCache: make(map[string]StreamInfo)}
	var jsonData = []byte(`{
		"url": "http://localhost/input",
		"callback_url": "http://localhost/callback",
		"output_locations": [
			{
				"type": "object_store",
				"url": "memory://localhost/output",
 				"outputs": {
					"source_segments": true
				}
			},
			{
				"type": "pinata",
				"pinata_access_key": "abc",
 				"outputs": {
					"transcoded_segments": true
				}
			}
		]
	}`)

	router := httprouter.New()

	req, _ := http.NewRequest("POST", "/api/vod", bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	router.POST("/api/vod", catalystApiHandlers.UploadVOD())
	router.ServeHTTP(rr, req)

	require.Equal(rr.Result().StatusCode, 200)
	require.Equal(rr.Body.String(), "2")
}

func TestInvalidPayloadVODUploadHandler(t *testing.T) {
	require := require.New(t)

	catalystApiHandlers := CatalystAPIHandlersCollection{MistClient: StubMistClient{}}
	badRequests := [][]byte{
		// missing url
		[]byte(`{
			"callback_url": "http://localhost/callback",
			"output_locations": [ { "type": "object_store", "url": "memory://localhost/output" } ]
		}`),
		// missing callback_url
		[]byte(`{
			"url": "http://localhost/input",
			"output_locations": [ { "type": "object_store", "url": "memory://localhost/output" } ]
		}`),
		// missing output_locations
		[]byte(`{
			"url": "http://localhost/input",
			"callback_url": "http://localhost/callback"
		}`),
		// invalid url
		[]byte(`{
			"url": "x://}]:&7@localhost/",
			"callback_url": "http://localhost/callback",
			"output_locations": [ { "type": "object_store", "url": "memory://localhost/output" } ]
		}`),
		// invalid callback_url
		[]byte(`{
			"url": "http://localhost/input",
			"callback_url": "x://}]:&7@localhost/",
			"output_locations": [ { "type": "object_store", "url": "memory://localhost/output" } ]
		}`),
		// invalid output_location's object_store url
		[]byte(`{
			"url": "http://localhost/input",
			"callback_url": "http://localhost/callback",
			"output_locations": [ { "type": "object_store", "url": "x://}]:&7@localhost/" } ]
		}`),
		// invalid output_location type
		[]byte(`{
			"url": "http://localhost/input",
			"callback_url": "http://localhost/callback",
			"output_locations": [ { "type": "foo", "url": "http://localhost/" } ]
		}`),
		// invalid output_location's pinata params
		[]byte(`{
			"url": "http://localhost/input",
			"callback_url": "http://localhost/callback",
			"output_locations": [ { "type": "pinata", "pinata_access_key": "" } ]
		}`),
	}

	router := httprouter.New()

	router.POST("/api/vod", catalystApiHandlers.UploadVOD())
	for _, payload := range badRequests {
		req, _ := http.NewRequest("POST", "/api/vod", bytes.NewBuffer(payload))
		req.Header.Set("Content-Type", "application/json")

		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		require.Equal(rr.Result().StatusCode, 400)
	}
}

func TestWrongContentTypeVODUploadHandler(t *testing.T) {
	require := require.New(t)

	catalystApiHandlers := CatalystAPIHandlersCollection{MistClient: StubMistClient{}}
	var jsonData = []byte(`{
		"url": "http://localhost/input",
		"callback_url": "http://localhost/callback",
		"output_locations": [
			{
				"type": "object_store",
				"url": "http://localhost/"
			}
		]
	}`)

	router := httprouter.New()
	req, _ := http.NewRequest("POST", "/api/vod", bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "json")

	rr := httptest.NewRecorder()
	router.POST("/api/vod", catalystApiHandlers.UploadVOD())
	router.ServeHTTP(rr, req)

	require.Equal(rr.Result().StatusCode, 415)
	require.JSONEq(rr.Body.String(), `{"error": "Requires application/json content type"}`)
}
