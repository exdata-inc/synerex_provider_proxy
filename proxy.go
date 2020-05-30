package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"

	"google.golang.org/grpc"

	api "github.com/synerex/synerex_api"
	nodeapi "github.com/synerex/synerex_nodeapi"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"
)

var (
	port            = flag.Int("port", 18000, "The Proxy Listening Port")
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	cluster_id      = flag.Int("cluster_id", 0, "ClusterId for The Synerex Server")
	channel         = flag.Int("channel", 1, "Channel")
	name            = flag.String("name", "Proxy", "Provider Name")
	sxServerAddress string
	sclient         *sxutil.SXServiceClient
)

func init() {
	sclient = nil
}

type proxyInfo struct {
}

func (p proxyInfo) NotifyDemand(ctx context.Context, dm *api.Demand) (*api.Response, error) {
	return sclient.Client.NotifyDemand(ctx, dm)
}

func (p proxyInfo) NotifySupply(ctx context.Context, sp *api.Supply) (*api.Response, error) {
	return sclient.Client.NotifySupply(ctx, sp)
}

func (p proxyInfo) ProposeDemand(ctx context.Context, dm *api.Demand) (*api.Response, error) {
	return sclient.Client.ProposeDemand(ctx, dm)
}

func (p proxyInfo) ProposeSupply(ctx context.Context, sp *api.Supply) (*api.Response, error) {
	return sclient.Client.ProposeSupply(ctx, sp)
}

func (p proxyInfo) SelectSupply(ctx context.Context, target *api.Target) (*api.ConfirmResponse, error) {
	return sclient.Client.SelectSupply(ctx, target)
}

func (p proxyInfo) SelectDemand(ctx context.Context, target *api.Target) (*api.ConfirmResponse, error) {
	return sclient.Client.SelectDemand(ctx, target)
}

func (p proxyInfo) Confirm(ctx context.Context, target *api.Target) (*api.Response, error) {
	return sclient.Client.Confirm(ctx, target)
}

func (p proxyInfo) SubscribeDemand(ch *api.Channel, stream api.Synerex_SubscribeDemandServer) error {
	ctx := context.Background()
	dmc, err := sclient.Client.SubscribeDemand(ctx, ch)

	if err != nil {
		log.Printf("SubscribeDemand Error %v", err)
		return err
	}

	for {
		var dm *api.Demand
		dm, err = dmc.Recv() // receive Demand
		if err != nil {
			if err == io.EOF {
				log.Print("End Demand subscribe OK")
			} else {
				log.Printf("SXServiceClient SubscribeDemand error [%v]", err)
			}
			break
		}
		stream.Send(dm)
	}

	return err
}

func (p proxyInfo) SubscribeSupply(ch *api.Channel, stream api.Synerex_SubscribeSupplyServer) error {
	ctx := context.Background()
	spc, err := sclient.Client.SubscribeSupply(ctx, ch)

	if err != nil {
		log.Printf("SubscribeSupply Error %v", err)
		return err
	}

	for {
		var sp *api.Supply
		sp, err = spc.Recv() // receive Demand
		if err != nil {
			if err == io.EOF {
				log.Print("End Supply subscribe OK")
			} else {
				log.Printf("SXServiceClient SubscribeSupply error [%v]", err)
			}
			break
		}
		stream.Send(sp)
	}

	return err
}

func (p proxyInfo) SubscribeMbus(mb *api.Mbus, stream api.Synerex_SubscribeMbusServer) error {
	ctx := context.Background()
	mbc, err := sclient.Client.SubscribeMbus(ctx, mb)

	if err != nil {
		log.Printf("SubscribeMbus Error %v", err)
		return err
	}

	for {
		var mes *api.MbusMsg
		mes, err = mbc.Recv() // receive Demand
		if err != nil {
			if err == io.EOF {
				log.Print("End Mbus subscribe OK")
			} else {
				log.Printf("SXServiceClient SubscribeMbus error [%v]", err)
			}
			break
		}
		stream.Send(mes)
	}

	return err
}

func (p proxyInfo) SendMsg(ctx context.Context, mb *api.MbusMsg) (*api.Response, error) {
	return sclient.Client.SendMsg(ctx, mb)
}

func (p proxyInfo) CloseMbus(ctx context.Context, mb *api.Mbus) (*api.Response, error) {
	return sclient.Client.CloseMbus(ctx, mb)
}

func (p proxyInfo) SubscribeGateway(*api.GatewayInfo, api.Synerex_SubscribeGatewayServer) error {
	panic("implement me")
}

func (p proxyInfo) ForwardToGateway(context.Context, *api.GatewayMsg) (*api.Response, error) {
	panic("implement me")
}

func (p proxyInfo) CloseDemandChannel(ctx context.Context, ch *api.Channel) (*api.Response, error) {
	return sclient.Client.CloseDemandChannel(ctx, ch)
}

func (p proxyInfo) CloseSupplyChannel(ctx context.Context, ch *api.Channel) (*api.Response, error) {
	return sclient.Client.CloseSupplyChannel(ctx, ch)
}

func (p proxyInfo) CloseAllChannels(ctx context.Context, id *api.ProviderID) (*api.Response, error) {
	return sclient.Client.CloseAllChannels(ctx, id)
}

func newProxyInfo() *proxyInfo {
	var pi proxyInfo
	s := &pi

	return s
}

func prepareGrpcServer(pi *proxyInfo, opts ...grpc.ServerOption) *grpc.Server {
	server := grpc.NewServer(opts...)
	api.RegisterSynerexServer(server, pi)
	return server
}

func providerInit(command nodeapi.KeepAliveCommand, ret string) {
	channelTypes := []uint32{uint32(*channel)}
	sxo := &sxutil.SxServerOpt{
		NodeType:  nodeapi.NodeType_PROVIDER,
		ClusterId: int32(*cluster_id),
		AreaId:    "Default",
	}
	// obtain synerex server address from nodeserv
	srv, err := sxutil.RegisterNodeWithCmd(*nodesrv, *name, channelTypes, sxo, providerInit)
	if err != nil {
		log.Fatal("Can't register node...")
	}
	log.Printf("Connecting Server [%s]\n", srv)

	sxServerAddress = srv
	client := sxutil.GrpcConnectServer(srv)
	argJson := fmt.Sprintf("{Client:Simple}")
	sclient = sxutil.NewSXServiceClient(client, pbase.RIDE_SHARE, argJson)
}

func main() {
	log.Printf("ProxyProvider(%s) built %s sha1 %s", sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)

	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	providerInit(nodeapi.KeepAliveCommand_NONE, "")

	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *port))

	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	var opts []grpc.ServerOption

	s := newProxyInfo()
	grpcServer := prepareGrpcServer(s, opts...)
	log.Printf("Start Synerex Server, connection waiting at port :%d ...", *port)
	serr := grpcServer.Serve(lis)
	log.Printf("Should not arrive here.. server closed. %v", serr)

	sxutil.CallDeferFunctions() // cleanup!

}
