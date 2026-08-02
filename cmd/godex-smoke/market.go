package main

// Market-data watch mode: subscribes the venue's public book stream and polls
// funding, printing top-of-book and funding observations until the duration
// elapses or the process is interrupted. Read-only — no credentials involved.
// This is the adoption check for the market-data layer, the counterpart of
// the execution smoke scenarios.

import (
	"context"
	"fmt"
	"time"

	"github.com/DaisukeYoda/godex"
	"github.com/DaisukeYoda/godex/dydx"
	"github.com/DaisukeYoda/godex/lighter"
)

const (
	// tobLogInterval throttles top-of-book printing; the raw stream is far
	// chattier than a human-readable log wants.
	tobLogInterval = 1 * time.Second
	// fundingPollInterval matches the cadence a funding-carry consumer uses.
	fundingPollInterval = 15 * time.Second
)

// buildMarketPair constructs the venue's stream and polled client.
func buildMarketPair(opts options) (godex.MarketStream, godex.MarketDataClient, error) {
	switch opts.venue {
	case venueLighter:
		stream, err := lighter.NewMarketStream(lighter.MarketStreamConfig{
			Symbol:     godex.Symbol(opts.symbol),
			MarketID:   opts.marketID,
			PriceScale: opts.priceScale,
			SizeScale:  opts.sizeScale,
			Network:    lighter.Network(opts.network),
		})
		if err != nil {
			return nil, nil, err
		}
		data, err := lighter.NewMarketData(lighter.MarketDataConfig{
			Symbol:   godex.Symbol(opts.symbol),
			MarketID: opts.marketID,
			Network:  lighter.Network(opts.network),
		})
		if err != nil {
			return nil, nil, err
		}
		return stream, data, nil
	case venueDydx:
		stream, err := dydx.NewMarketStream(dydx.MarketStreamConfig{
			Symbol:     godex.Symbol(opts.symbol),
			Ticker:     opts.ticker,
			PriceScale: opts.priceScale,
			SizeScale:  opts.sizeScale,
			Network:    dydx.Network(opts.network),
		})
		if err != nil {
			return nil, nil, err
		}
		data, err := dydx.NewMarketData(dydx.MarketDataConfig{
			Symbol:  godex.Symbol(opts.symbol),
			Ticker:  opts.ticker,
			Network: dydx.Network(opts.network),
		})
		if err != nil {
			return nil, nil, err
		}
		return stream, data, nil
	default:
		return nil, nil, fmt.Errorf("-market-watch supports venues %s and %s", venueLighter, venueDydx)
	}
}

func runMarketWatch(ctx context.Context, opts options) error {
	stream, data, err := buildMarketPair(opts)
	if err != nil {
		return err
	}
	if opts.watchDuration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.watchDuration)
		defer cancel()
	}

	if err := stream.Start(ctx); err != nil {
		return err
	}
	defer func() { _ = stream.Close() }()
	logf("market watch started: venue=%s symbol=%s", opts.venue, opts.symbol)

	fundingTicker := time.NewTicker(fundingPollInterval)
	defer fundingTicker.Stop()
	logFunding := func() {
		pollCtx, cancel := context.WithTimeout(ctx, tobRequestTimeout)
		defer cancel()
		rate, err := data.FundingRate(pollCtx)
		if err != nil {
			logf("funding poll failed: %v", err)
			return
		}
		logf("funding rate=%s interval=%dh", rate.Rate.String(), rate.IntervalHours)
		stats, err := data.MarketStats(pollCtx)
		if err != nil {
			logf("stats poll failed: %v", err)
			return
		}
		logf("stats oiUsd=%s volume24hUsd=%s", stats.OpenInterestUSD.String(), stats.Volume24hUSD.String())
	}
	logFunding()

	var books, dropsThrottled int
	var lastTOBLog time.Time
	for {
		select {
		case <-ctx.Done():
			logf("market watch done: books=%d (throttled from log: %d)", books, dropsThrottled)
			return nil
		case <-fundingTicker.C:
			logFunding()
		case event, ok := <-stream.Events():
			if !ok {
				return fmt.Errorf("market stream closed unexpectedly")
			}
			switch event := event.(type) {
			case godex.MarketConnectedEvent:
				logf("stream connected")
			case godex.MarketDisconnectedEvent:
				logf("stream disconnected")
			case godex.BookSnapshotEvent:
				books++
				if time.Since(lastTOBLog) < tobLogInterval {
					dropsThrottled++
					continue
				}
				lastTOBLog = time.Now()
				logTOB(event.Book)
			default:
				return fmt.Errorf("unknown market event %T", event)
			}
		}
	}
}

func logTOB(book godex.OrderBook) {
	bid, ask := "-", "-"
	bidSize, askSize := "-", "-"
	if len(book.Bids) > 0 {
		bid, bidSize = book.Bids[0].Price.String(), book.Bids[0].Size.String()
	}
	if len(book.Asks) > 0 {
		ask, askSize = book.Asks[0].Price.String(), book.Asks[0].Size.String()
	}
	logf("tob bid=%s (%s) ask=%s (%s) levels=%d/%d",
		bid, bidSize, ask, askSize, len(book.Bids), len(book.Asks))
}
