package proxy

import (
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/outbound"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/routing"
	"github.com/sagernet/sing-box/adapter"
)

type routedOutbound struct {
	Route    routing.RouteResult
	Entry    *node.NodeEntry
	Outbound adapter.Outbound
}

func resolveRoutedOutbound(
	router *routing.Router,
	pool outbound.PoolAccessor,
	platformName string,
	account string,
	target string,
) (routedOutbound, *ProxyError) {
	result, err := router.RouteRequestForProxy(platformName, account, target)
	if err != nil {
		return routedOutbound{}, mapRouteError(err)
	}
	return bindRoutedOutbound(result, pool)
}

func resolveRoutedOutboundForPlatform(
	router *routing.Router,
	pool outbound.PoolAccessor,
	plat *platform.Platform,
	account string,
	target string,
) (routedOutbound, *ProxyError) {
	result, err := router.RouteRequestForProxyForPlatform(plat, account, target)
	if err != nil {
		return routedOutbound{}, mapRouteError(err)
	}
	return bindRoutedOutbound(result, pool)
}

func bindRoutedOutbound(result routing.RouteResult, pool outbound.PoolAccessor) (routedOutbound, *ProxyError) {

	selectedEntry := result.SelectedEntry()
	if selectedEntry == nil {
		return routedOutbound{}, ErrNoAvailableNodes
	}
	entry, ok := pool.GetEntry(result.NodeHash)
	if !ok || entry != selectedEntry || entry == nil || !entry.IsHealthy() || pool.IsNodeDisabled(result.NodeHash) {
		return routedOutbound{}, ErrNoAvailableNodes
	}
	leasedOutbound, ready := entry.NewLeasedOutbound()
	if !ready {
		return routedOutbound{}, ErrNoAvailableNodes
	}

	return routedOutbound{
		Route:    result,
		Entry:    entry,
		Outbound: leasedOutbound,
	}, nil
}
