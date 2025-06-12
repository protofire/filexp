package main

import (
	"regexp"
	"strings"

	"code.riba.cloud/go/toolbox-interplanetary/fil"
	"github.com/aschmahmann/filexp/internal/bitswap"
	ipld "github.com/aschmahmann/filexp/internal/ipld"
	filabi "github.com/filecoin-project/go-state-types/abi"
	lchtypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"

	// force bundle load, needed for actors.GetActorCodeID() to work
	// DO NOT REMOVE as nothing will work
	_ "github.com/filecoin-project/lotus/build"
)

func stringSliceMap(ss []string, f func(string) string) []string {
	ssout := make([]string, len(ss))
	for i, s := range ss {
		ssout[i] = f(s)
	}
	return ssout
}

func getAnchorPoint(cctx *cli.Context) (*ipld.CountingBlockGetter, *lchtypes.TipSet, error) {
	log.Debug("getAnchorPoint: starting")

	sourceSelect := []string{"car", "rpc-endpoint", "rpc-fullnode"}

	var countHeadSources int
	for _, s := range sourceSelect {
		if cctx.IsSet(s) {
			log.Debugf("getAnchorPoint: source set: --%s", s)
			countHeadSources++
		}
	}
	if countHeadSources == 0 && cctx.Bool("trust-chainlove") {
		log.Debug("getAnchorPoint: no source set, using --trust-chainlove fallback")
		countHeadSources++
		if err := cctx.Set("rpc-endpoint", chainLoveURL); err != nil {
			log.Debugf("getAnchorPoint: failed to set rpc-endpoint from trust-chainlove: %v", err)
			return nil, nil, err
		}
	}

	if countHeadSources > 1 {
		log.Debug("getAnchorPoint: multiple sources specified, returning error")
		return nil, nil, xerrors.Errorf(
			"you can not specify more than one CurrentTipsetSource of: %s",
			strings.Join(stringSliceMap(sourceSelect, func(s string) string { return "--" + s }), ", "),
		)
	} else if countHeadSources == 0 && !cctx.IsSet("tipset-cids") {
		log.Debug("getAnchorPoint: no source and no --tipset-cids specified, returning error")
		return nil, nil, xerrors.Errorf(
			"you have not specified any CurrentTipsetSource (one of %s), as an alternative you must provide the tipset explicitly via '--tipset-cids'",
			strings.Join(stringSliceMap(sourceSelect, func(s string) string { return "--" + s }), ", "),
		)
	}

	rpcAddr := cctx.String("rpc-fullnode")
	if rpcAddr == "" {
		rpcAddr = cctx.String("rpc-endpoint")
	}
	log.Debugf("getAnchorPoint: using rpc address: %s", rpcAddr)

	ctx := cctx.Context
	var err error
	var bg *ipld.CountingBlockGetter
	var tsk *lchtypes.TipSetKey
	var ts *lchtypes.TipSet

	// supplied TSK takes precedence
	if cctx.IsSet("tipset-cids") {
		log.Debug("getAnchorPoint: using --tipset-cids to build tipset key")
		cidStrs := cctx.StringSlice("tipset-cids")
		var cids []cid.Cid
		for _, s := range cidStrs {
			for _, ss := range regexp.MustCompile(`[\s,:;]`).Split(s, -1) {
				if ss == "" {
					continue
				}
				c, err := cid.Decode(ss)
				if err != nil {
					log.Debugf("getAnchorPoint: failed to decode cid %s: %v", ss, err)
					return nil, nil, err
				}
				cids = append(cids, c)
			}
		}
		tskv := lchtypes.NewTipSetKey(cids...)
		tsk = &tskv
	}

	if cctx.IsSet("car") {
		log.Debugf("getAnchorPoint: using car file: %s", cctx.String("car"))
		var carTsk *lchtypes.TipSetKey
		bg, carTsk, err = ipld.GetStateFromCar(ctx, cctx.String("car"))
		if err != nil {
			log.Debugf("getAnchorPoint: failed to load car file: %v", err)
			return nil, nil, err
		}
		if tsk == nil {
			log.Debug("getAnchorPoint: setting tipset key from car file")
			tsk = carTsk
		}
	} else if rpcAddr != "" {
		log.Debugf("getAnchorPoint: connecting to Lotus RPC at %s", rpcAddr)
		lApi, apiCloser, err := fil.NewLotusDaemonAPIClientV0(ctx, rpcAddr, 0, "")
		if err != nil {
			log.Debugf("getAnchorPoint: failed to connect to Lotus RPC: %v", err)
			return nil, nil, err
		}
		go func() {
			<-ctx.Done()
			apiCloser()
		}()

		if tsk == nil {
			log.Debugf("getAnchorPoint: fetching tipset from Lotus RPC with lookback-epochs=%d", cctx.Uint("lookback-epochs"))
			ts, err = fil.GetTipset(ctx, lApi, filabi.ChainEpoch(cctx.Uint("lookback-epochs")))
			if err != nil {
				log.Debugf("getAnchorPoint: failed to get tipset from RPC: %v", err)
				return nil, nil, err
			}
		}

		if cctx.IsSet("rpc-fullnode") {
			log.Debug("getAnchorPoint: using RPC as block source")
			bg = &ipld.CountingBlockGetter{IpldBlockstore: &ipld.FilRpcBs{Rpc: lApi}}
		}
	}

	// If no block sources available, fall back to bitswap
	if bg == nil {
		log.Debug("getAnchorPoint: no block getter found, falling back to bitswap")
		bg, err = bitswap.InitBitswapGetter(ctx)
		if err != nil {
			log.Debugf("getAnchorPoint: failed to initialize bitswap: %v", err)
			return nil, nil, err
		}
	}

	// If tipset wasn't resolved directly, build it from headers
	if ts == nil {
		log.Debug("getAnchorPoint: assembling tipset from headers")
		eg, ctx := errgroup.WithContext(ctx)
		eg.SetLimit(8)

		hdrs := make([]*lchtypes.BlockHeader, len(tsk.Cids()))
		for i, c := range tsk.Cids() {
			i, c := i, c // capture loop vars
			eg.Go(func() error {
				log.Debugf("getAnchorPoint: fetching block header for cid %s", c)
				b, err := bg.Get(ctx, c)
				if err != nil {
					log.Debugf("getAnchorPoint: failed to get block %s: %v", c, err)
					return err
				}
				hdrs[i], err = lchtypes.DecodeBlock(b.RawData())
				if err != nil {
					log.Debugf("getAnchorPoint: failed to decode block %s: %v", c, err)
				}
				return err
			})
		}

		if err = eg.Wait(); err != nil {
			log.Debugf("getAnchorPoint: error waiting for block headers: %v", err)
			return nil, nil, err
		}

		ts, err = lchtypes.NewTipSet(hdrs)
		if err != nil {
			log.Debugf("getAnchorPoint: failed to create tipset: %v", err)
			return nil, nil, err
		}
	}

	log.Infof("gathering results from StateRoot %s referenced by tipset at height %d (%s) %s", ts.ParentState(), ts.Height(), fil.ClockMainnet.EpochToTime(ts.Height()), ts.Cids())

	return bg, ts, nil
}
