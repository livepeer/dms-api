package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/livepeer/catalyst-api/pipeline"
)

// TODO add back in
// DebugEndpointListenAndServe provides a handler to allow us to check internal state of the app for debugging
func DebugEndpointListenAndServe(port int, vodEngine *pipeline.Coordinator) error {
	if vodEngine == nil {
		return errors.New("vodEngine was nil")
	}
	listen := fmt.Sprintf("0.0.0.0:%d", port)
	http.Handle("/jobs", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cache := vodEngine.Jobs.UnittestIntrospection()
		bs, err := json.Marshal(cache)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("failed to marshal json: " + err.Error()))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(bs)
	}))
	http.Handle("/jobscount", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cache := vodEngine.Jobs.UnittestIntrospection()

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(fmt.Sprintf("%d", len(*cache))))
	}))

	return http.ListenAndServe(listen, nil)
}
