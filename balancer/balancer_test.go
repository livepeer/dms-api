package balancer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func start(t *testing.T) (*BalancerImpl, *mockMistUtilLoad) {
	mul := newMockMistUtilLoad(t)

	b := &BalancerImpl{
		config:   &Config{},
		cmd:      nil,
		endpoint: mul.Server.URL,
	}
	// Mock startup loop
	b.startupOnce.Do(func() {})
	return b, mul
}

func TestGetMistUtilLoadServers(t *testing.T) {
	bal, mul := start(t)
	defer mul.Close()
	mul.BalancedHosts = map[string]string{
		"http://one.example.com:4242":   "Online",
		"http://two.example.com:4242":   "Online",
		"http://three.example.com:4242": "Online",
		"http://four.example.com:4242":  "Online",
	}
	servers, err := bal.getMistLoadBalancerServers(context.Background())
	require.NoError(t, err)
	require.Len(t, servers, 4)
	requireKeysEqual(t, servers, mul.BalancedHosts)
}

// Test that our local server gets converted to our node name on the way out of MistUtilLoad
func TestConvertLocalFromMist(t *testing.T) {
	bal, mul := start(t)
	defer mul.Close()
	bal.mistAddr = "http://127.0.0.1:4242"
	bal.config.NodeName = "example.com"
	bal.config.MistLoadBalancerTemplate = "https://%s:1234"
	mul.BalancedHosts = map[string]string{}
	mul.BalancedHosts[bal.mistAddr] = "Online"
	servers, err := bal.getMistLoadBalancerServers(context.Background())
	require.NoError(t, err)
	require.Len(t, servers, 1)
	_, ok := servers["https://example.com:1234"]
	require.True(t, ok, "incorrect response from getMistLoadBalancerServers: %v", servers)
}

func TestSetMistUtilLoadServers(t *testing.T) {
	bal, mul := start(t)
	defer mul.Close()
	bal.config.MistLoadBalancerTemplate = "https://%s:4321"
	hosts := []string{
		"a.example.com",
		"b.example.com",
		"c.example.com",
		"d.example.com",
	}
	for _, host := range hosts {
		bal.changeLoadBalancerServers(context.Background(), host, "add")
	}
	keys := toSortedKeys(t, mul.BalancedHosts)
	require.Equal(t, keys, []string{
		"https://a.example.com:4321",
		"https://b.example.com:4321",
		"https://c.example.com:4321",
		"https://d.example.com:4321",
	})

	bal.changeLoadBalancerServers(context.Background(), "c.example.com", "del")
	keys = toSortedKeys(t, mul.BalancedHosts)
	require.Equal(t, keys, []string{
		"https://a.example.com:4321",
		"https://b.example.com:4321",
		"https://d.example.com:4321",
	})
}

type mockMistUtilLoad struct {
	HttpCalls     int
	BalancedHosts map[string]string
	Server        *httptest.Server
}

func newMockMistUtilLoad(t *testing.T) *mockMistUtilLoad {
	mul := &mockMistUtilLoad{}
	ts := httptest.NewServer(mul.Handle(t))
	mul.Server = ts
	mul.BalancedHosts = map[string]string{}
	return mul
}

func (mul *mockMistUtilLoad) Handle(t *testing.T) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queryVals := r.URL.Query()
		if len(queryVals) > 1 {
			require.Fail(t, "Got more than one query parameter!")
			return
		}
		if len(queryVals) == 0 {
			// Default balancer implementation
			panic("unimplemented")
		}

		// Listing servers - ?lstservers=1
		if vals, ok := queryVals["lstservers"]; ok {
			require.Len(t, vals, 1)
			require.Equal(t, vals[0], "1")
			b, err := json.Marshal(mul.BalancedHosts)
			require.NoError(t, err)
			w.Write(b)
			return
		}

		// Adding servers - ?addserver=server
		if vals, ok := queryVals["addserver"]; ok {
			require.Len(t, vals, 1)
			host := vals[0]
			mul.BalancedHosts[host] = "Online"
			return
		}

		// Deleting servers - ?delserver=server
		if vals, ok := queryVals["delserver"]; ok {
			require.Len(t, vals, 1)
			host := vals[0]
			delete(mul.BalancedHosts, host)
			return
		}
		require.Fail(t, fmt.Sprintf("unimplemented mock query parameter: %s", r.URL.RawQuery))
	})
}

func (mul *mockMistUtilLoad) Close() {
	mul.Server.Close()
}

func toSortedKeys(t *testing.T, m any) []string {
	value := reflect.ValueOf(m)
	if value.Kind() != reflect.Map {
		require.Fail(t, fmt.Sprintf("argument is not a map: %v", m))
		return []string{}
	}
	s := []string{}
	for _, key := range value.MapKeys() {
		s = append(s, key.String())
	}
	sort.Strings(s)
	return s
}

// Check that two maps have equal keys (values don't matter)
func requireKeysEqual(t *testing.T, m1, m2 any) {
	s1 := toSortedKeys(t, m1)
	s2 := toSortedKeys(t, m2)
	require.Equal(t, s1, s2)
}
