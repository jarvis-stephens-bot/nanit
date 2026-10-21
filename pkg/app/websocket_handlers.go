package app

import (
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gregory-m/nanit/pkg/baby"
	"github.com/gregory-m/nanit/pkg/client"
	"github.com/gregory-m/nanit/pkg/utils"
)

func processSensorData(babyUID string, sensorData []*client.SensorData, stateManager *baby.StateManager) {
	// Parse sensor update
	stateUpdate := baby.State{}
	for _, sensorDataSet := range sensorData {
		if *sensorDataSet.SensorType == client.SensorType_TEMPERATURE {
			stateUpdate.SetTemperatureMilli(*sensorDataSet.ValueMilli)
		} else if *sensorDataSet.SensorType == client.SensorType_HUMIDITY {
			stateUpdate.SetHumidityMilli(*sensorDataSet.ValueMilli)
		} else if *sensorDataSet.SensorType == client.SensorType_NIGHT {
			stateUpdate.SetIsNight(*sensorDataSet.Value == 1)
		}
	}

	stateManager.Update(babyUID, stateUpdate)
}

// Backoff bounds for the "Request timeout" retry path.
//
// Without a delay on that path the loop re-issues PUT_STREAMING roughly every
// 30s indefinitely. Each attempt is counted server-side as a Mobile App
// connection, so a camera that is slow to respond makes the client exhaust the
// account's own connection budget, which then surfaces as
// "Forbidden: Number of Mobile App connections above limit, declining connection"
// — i.e. the limit error is self-inflicted rather than caused by other clients.
//
// Observed on a two-camera deployment over 16 days before this change:
// 5,212 "Streaming request timeout" retries and 5,732 total stream requests
// yielding only 70 successes.
const (
	streamRetryInitialBackoff = 30 * time.Second
	streamRetryMaxBackoff     = 5 * time.Minute
)

// sendStopStreaming sends a single PUT_STREAMING STOPPED request and does not
// retry or block the caller on the camera's response.
//
// A publisher that dies without a clean FIN/RST (EOF, reset, or a read
// timeout) leaves our WebSocket session registered as still streaming from
// Nanit's point of view. Nanit's cloud then holds the "Mobile App connection"
// slot for that session until its own stale-session timeout expires — which
// is unbounded and can take minutes to hours (see
// _System/research/nanit-lockout-reliability-2026-10.md). Explicitly telling
// the camera the session stopped gives Nanit's cloud an immediate reason to
// release the slot instead of waiting out that timeout, mirroring the fix
// GhostOnyx/homebridge-nanit-pro shipped in v1.1.13 for the same failure mode.
//
// We do not wait for a response. Live observation (13/13 attempts over
// 2026-10-18 to 2026-10-21, see
// _System/research/nanit-lockout-drops-and-stop-timeout-2026-10-21.md) found
// the camera never acknowledges a STOPPED for a session it already considers
// dead — including on a freshly reconnected, otherwise-healthy websocket — so
// the prior 10s blocking wait only ever timed out, adding dead time to every
// reconnect cycle for no information gained. homebridge-nanit-pro's own
// reference fix for this same failure mode (v1.1.13) does not wait for a
// response either: it writes the STOPPED message and moves on. Sending the
// request is what matters to Nanit's cloud; waiting for its ack is not.
func sendStopStreaming(babyUID string, targetURL string, conn *client.WebsocketConnection) {
	log.Info().Str("target", targetURL).Msg("Sending explicit stop for dead publisher before reconnecting")

	conn.SendRequest(client.RequestType_PUT_STREAMING, &client.Request{
		Streaming: &client.Streaming{
			Id:       client.StreamIdentifier(client.StreamIdentifier_MOBILE).Enum(),
			RtmpUrl:  utils.ConstRefStr(targetURL),
			Status:   client.Streaming_Status(client.Streaming_STOPPED).Enum(),
			Attempts: utils.ConstRefInt32(1),
		},
	})
}

func requestLocalStreaming(babyUID string, targetURL string, streamingStatus client.Streaming_Status, conn *client.WebsocketConnection, stateManager *baby.StateManager) {
	timeoutBackoff := streamRetryInitialBackoff

	for {
		switch streamingStatus {
		case client.Streaming_STARTED:
			log.Info().Str("target", targetURL).Msg("Requesting local streaming")
		case client.Streaming_PAUSED:
			log.Info().Str("target", targetURL).Msg("Pausing local streaming")
		case client.Streaming_STOPPED:
			log.Info().Str("target", targetURL).Msg("Stopping local streaming")
		}

		awaitResponse := conn.SendRequest(client.RequestType_PUT_STREAMING, &client.Request{
			Streaming: &client.Streaming{
				Id:       client.StreamIdentifier(client.StreamIdentifier_MOBILE).Enum(),
				RtmpUrl:  utils.ConstRefStr(targetURL),
				Status:   client.Streaming_Status(streamingStatus).Enum(),
				Attempts: utils.ConstRefInt32(1),
			},
		})

		_, err := awaitResponse(30 * time.Second)

		if err != nil {
			if err.Error() == "Forbidden: Number of Mobile App connections above limit, declining connection" {
				log.Warn().Err(err).Msg("Too many app connections, waiting for local connection to become available...")
				stateManager.Update(babyUID, *baby.NewState().SetStreamRequestState(baby.StreamRequestState_RequestFailed))
				sendStopStreaming(babyUID, targetURL, conn)
                                time.Sleep(300 * time.Second)
				continue
			} else if err.Error() != "Request timeout" {
				if stateManager.GetBabyState(babyUID).GetStreamState() == baby.StreamState_Alive {
					log.Info().Err(err).Msg("Failed to request local streaming, but stream seems to be alive from previous run")
				} else if stateManager.GetBabyState(babyUID).GetStreamState() == baby.StreamState_Unhealthy {
					log.Error().Err(err).Msg("Failed to request local streaming and stream seems to be dead")
					stateManager.Update(babyUID, *baby.NewState().SetStreamRequestState(baby.StreamRequestState_RequestFailed))
				} else {
					log.Warn().Err(err).Msg("Failed to request local streaming, awaiting stream health check")
					stateManager.Update(babyUID, *baby.NewState().SetStreamRequestState(baby.StreamRequestState_RequestFailed))
				}

				return
			}

			if !stateManager.GetBabyState(babyUID).GetIsWebsocketAlive() {
				return
			}

			log.Warn().Dur("backoff", timeoutBackoff).Msg("Streaming request timeout, trying again")
			time.Sleep(timeoutBackoff)

			if timeoutBackoff < streamRetryMaxBackoff {
				timeoutBackoff *= 2
				if timeoutBackoff > streamRetryMaxBackoff {
					timeoutBackoff = streamRetryMaxBackoff
				}
			}

		} else {
			timeoutBackoff = streamRetryInitialBackoff
			log.Info().Msg("Local streaming successfully requested")
			stateManager.Update(babyUID, *baby.NewState().SetStreamRequestState(baby.StreamRequestState_Requested))
			return
		}
	}
}
