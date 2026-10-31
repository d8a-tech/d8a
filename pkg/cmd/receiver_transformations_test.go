package cmd

import (
	"context"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/dbip"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

type receiverTransformationStorage struct {
	batches chan []*hits.Hit
}

func (s receiverTransformationStorage) Push(batch []*hits.Hit) error {
	s.batches <- batch
	return nil
}

type receiverTransformationGeo struct {
	ips chan netip.Addr
}

func (g receiverTransformationGeo) Lookup(ip netip.Addr) (*dbip.LookupResult, error) {
	g.ips <- ip
	return &dbip.LookupResult{Country: "NL"}, nil
}

func TestBuildReceiverServerWiresTransformationsAndReusesGeo(t *testing.T) {
	// given
	configPath := writeConfigFile(t, `
transformations:
  rules:
    - expression: 'geo_country == "NL"'
      actions:
        - type: set
          target: {field: page_title}
          value: {string: early-transformed}
`)
	setConfigFileForTest(t, configPath)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	_, portString, err := net.SplitHostPort(address)
	require.NoError(t, err)
	port, err := strconv.Atoi(portString)
	require.NoError(t, err)
	require.NoError(t, listener.Close())
	args := []string{"d8a-test", "--config=" + configPath, "--" + protocolFlag.Name + "=ga4",
		"--" + propertyIDFlag.Name + "=property", "--" + propertySettingsIPMaskingLevelFlag.Name + "=1"}
	setCurrentRunArgsForTest(t, args)
	queue := receiverTransformationStorage{batches: make(chan []*hits.Hit, 1)}
	geo := receiverTransformationGeo{ips: make(chan netip.Addr, 1)}
	flags := append(newPropertySettingsFlagsForConfigTests(),
		&cli.IntFlag{Name: serverPortFlag.Name, Value: port},
		&cli.StringFlag{Name: serverHostFlag.Name, Value: "127.0.0.1"},
		&cli.IntFlag{Name: receiverMaxHitKbytesFlag.Name, Value: 1024},
	)
	app := &cli.Command{Name: "d8a-test", Flags: flags, Action: func(ctx context.Context, cmd *cli.Command) error {
		server := buildReceiverServer(cmd, queue, nil, geo)
		serverCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- server.Run(serverCtx) }()
		defer func() {
			cancel()
			require.NoError(t, <-done)
		}()
		client := &http.Client{Timeout: time.Second}
		defer client.CloseIdleConnections()
		require.Eventually(t, func() bool {
			request, requestErr := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+"/healthz", http.NoBody)
			require.NoError(t, requestErr)
			response, requestErr := client.Do(request)
			if requestErr != nil {
				return false
			}
			require.NoError(t, response.Body.Close())
			return response.StatusCode == http.StatusOK
		}, 5*time.Second, 10*time.Millisecond)
		// when
		request, requestErr := http.NewRequestWithContext(ctx, http.MethodGet,
			"http://"+address+"/g/collect?tid=measurement&cid=client&en=page_view&dt=original", http.NoBody)
		require.NoError(t, requestErr)
		request.Header.Set("User-Agent", "test-agent")
		response, requestErr := client.Do(request)
		require.NoError(t, requestErr)
		require.NoError(t, response.Body.Close())
		// then
		require.Equal(t, http.StatusNoContent, response.StatusCode)
		select {
		case batch := <-queue.batches:
			require.Len(t, batch, 1)
			assert.Equal(t, "early-transformed", batch[0].Request.QueryParams.Get("dt"))
			assert.Nil(t, batch[0].Request.Body)
		case <-time.After(time.Second):
			t.Fatal("receiver did not queue transformed hit")
		}
		select {
		case ip := <-geo.ips:
			assert.Equal(t, "127.0.0.0", ip.String())
		case <-time.After(time.Second):
			t.Fatal("receiver did not use the supplied geo provider")
		}
		return nil
	}}
	require.NoError(t, app.Run(context.Background(), args))
}
