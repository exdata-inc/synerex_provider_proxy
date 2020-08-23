package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"path"
	"sync"

	"google.golang.org/grpc"

	api "github.com/synerex/synerex_api"
	nodeapi "github.com/synerex/synerex_nodeapi"
	proto "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"
)

var (
	port            = flag.Int("port", 18000, "The Proxy Listening Port")
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	clusterId       = flag.Int("cluster_id", 0, "ClusterId for The Synerex Server")
	channel         = flag.Int("channel", 1, "Channel")
	name            = flag.String("name", "Proxy", "Provider Name")
	verbose         = flag.Bool("verbose", false, "Verbose message flag")
	sxServerAddress string
	sclient         *sxutil.SXServiceClient
	smu, dmu        sync.RWMutex
	supplyChs       [proto.ChannelTypeMax][]chan *api.Supply
	demandChs       [proto.ChannelTypeMax][]chan *api.Demand
)

func init() {
	sclient = nil
}

const MessageChannelBufferSize = 100 // same as synerex-server.go : 30

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

func removeDemandChannelFromSlice(sl []chan *api.Demand, c chan *api.Demand) []chan *api.Demand {
	for i, ch := range sl {
		if ch == c {
			return append(sl[:i], sl[i+1:]...)
		}
	}
	log.Printf("Cant find channel %v in removeChannel", c)
	return sl
}

func removeSupplyChannelFromSlice(sl []chan *api.Supply, c chan *api.Supply) []chan *api.Supply {
	for i, ch := range sl {
		if ch == c {
			return append(sl[:i], sl[i+1:]...)
		}
	}
	log.Printf("Cant find channel %v in removeChannel", c)
	return sl
}

func (p proxyInfo) SubscribeDemand(ch *api.Channel, stream api.Synerex_SubscribeDemandServer) error {
	ctx := context.Background()
	ch.ClientId = uint64(sclient.ClientID) // we need to set proper clientID

	dmu.Lock()
	if len(demandChs[ch.ChannelType]) == 0 { // if there is no subscriber.
		demCh := make(chan *api.Demand, MessageChannelBufferSize)
		demandChs[ch.ChannelType] = append(demandChs[ch.ChannelType], demCh)
		dmu.Unlock()
		dmc, err := sclient.Client.SubscribeDemand(ctx, ch)
		if err != nil {
			log.Printf("SubscribeDemand Error %v", err)
			dmu.Lock()
			demandChs[ch.ChannelType] = removeDemandChannelFromSlice(demandChs[ch.ChannelType], demCh)
			dmu.Unlock()
			return err
		} else {
			log.Printf("SubscribeDemand OK %v", ch)
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
			if *verbose {
				log.Printf("Demand:%d:%v", ch.ChannelType, dm)
			}
			dmu.Lock()
			chans := demandChs[ch.ChannelType]
			for i := range chans {
				if chans[i] == demCh {
					err = stream.Send(dm)
					if err != nil {
						log.Printf("Send Demand Error %v", err)
						dmu.Lock()
						demandChs[ch.ChannelType] = removeDemandChannelFromSlice(demandChs[ch.ChannelType], demCh)
						dmu.Unlock()
					}
				} else {
					chans[i] <- dm
				}
			}
			dmu.Unlock()
			if len(chans) == 0 { // if there is no receiver quit subscirption.
				// we need to send "No subcriber to server"
				err = dmc.CloseSend()
				break
			}
		}
		return err
	} else {
		log.Printf("No %d SubscribeDemand OK %v", len(demandChs[ch.ChannelType])+1, ch)
		demCh := make(chan *api.Demand, MessageChannelBufferSize)
		demandChs[ch.ChannelType] = append(demandChs[ch.ChannelType], demCh)
		dmu.Unlock()
		var err error
		for {
			dm := <-demCh // receive Supply
			err = stream.Send(dm)
			if err != nil {
				log.Printf("Send Demand Error %v", err)
				dmu.Lock()
				demandChs[ch.ChannelType] = removeDemandChannelFromSlice(demandChs[ch.ChannelType], demCh)
				dmu.Unlock()
				break
			}
		}
		return err
	}
}

func (p proxyInfo) SubscribeSupply(ch *api.Channel, stream api.Synerex_SubscribeSupplyServer) error {
	ctx := context.Background()
	ch.ClientId = uint64(sclient.ClientID) // we need to set proper clientID

	smu.Lock()
	supCh := make(chan *api.Supply, MessageChannelBufferSize)
	supplyChs[ch.ChannelType] = append(supplyChs[ch.ChannelType], supCh)
	smu.Unlock()
	if len(supplyChs[ch.ChannelType]) == 1 { // if there is no subscriber.
		spc, err := sclient.Client.SubscribeSupply(ctx, ch)
		if err != nil {
			log.Printf("SubscribeSupply Error %v", err)
			smu.Lock()
			supplyChs[ch.ChannelType] = removeSupplyChannelFromSlice(supplyChs[ch.ChannelType], supCh)
			smu.Unlock()
			return err
		} else {
			log.Printf("SubscribeSupply OK %v", ch)
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
			if *verbose {
				log.Printf("Supply:%d:%v", ch.ChannelType, sp)
			}
			smu.Lock()
			chans := supplyChs[ch.ChannelType]
			for i := range chans {
				if chans[i] == supCh {
					err = stream.Send(sp)
					if err != nil {
						log.Printf("Send Supply Error %v", err)
						smu.Lock()
						supplyChs[ch.ChannelType] = removeSupplyChannelFromSlice(supplyChs[ch.ChannelType], supCh)
						smu.Unlock()
					}
				} else {
					chans[i] <- sp
				}
			}
			smu.Unlock()
			if len(chans) == 0 { // if there is no receiver quit subscirption.
				// we need to send "No subcriber to server"
				err = spc.CloseSend()
				break
			}
		}
		log.Printf("SubscribeSupply main closed: %v", ch)
		return err
	} else {
		num := len(supplyChs[ch.ChannelType])
		log.Printf("No %d SubscribeSupply OK %v", num, ch)
		var err error = nil
		for {
			sp := <-supCh // receive Supply
			if sp != nil {
				err = stream.Send(sp)
				if err != nil {
					log.Printf("Send Supply Error %v", err)
					smu.Lock()
					supplyChs[ch.ChannelType] = removeSupplyChannelFromSlice(supplyChs[ch.ChannelType], supCh)
					smu.Unlock()
					break
				}
			} else {
				log.Printf("Channel %d closed", num)
				break
			}
		}
		log.Printf("SubscribeSupply %d closed : %v", num, ch)
		return err
	}

}

func (p proxyInfo) CreateMbus(ctx context.Context, mbOpt *api.MbusOpt) (*api.Mbus, error) {
	return sclient.Client.CreateMbus(ctx, mbOpt)
}

func (p proxyInfo) CloseMbus(ctx context.Context, mb *api.Mbus) (*api.Response, error) {
	return sclient.Client.CloseMbus(ctx, mb)
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
		//
		if *verbose {
			log.Printf("MBus:%v", mes)
		}

		stream.Send(mes)
	}

	return err
}

func (p proxyInfo) SendMbusMsg(ctx context.Context, mb *api.MbusMsg) (*api.Response, error) {
	return sclient.Client.SendMbusMsg(ctx, mb)
}

func (p proxyInfo) GetMbusState(ctx context.Context, mb *api.Mbus) (*api.MbusState, error) {
	return sclient.Client.GetMbusState(ctx, mb)
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
	smu.Lock()
	if len(supplyChs[ch.ChannelType]) > 0 {
		log.Printf("ClosingSupply %v", ch)
		for i := range supplyChs[ch.ChannelType] {
			close(supplyChs[ch.ChannelType][i])
		}
		supplyChs[ch.ChannelType] = make([]chan *api.Supply, 0, 1)
	}
	smu.Unlock()
	//	return &api.Response{Ok: true, Err: ""}, nil
	ch.ClientId = uint64(sclient.ClientID)
	return sclient.Client.CloseSupplyChannel(ctx, ch)
}

func (p proxyInfo) CloseAllChannels(ctx context.Context, id *api.ProviderID) (*api.Response, error) {
	// special treatment for Proxy. We do not need to disconnect with server.

	return sclient.Client.CloseAllChannels(ctx, id)
}

func newProxyInfo() *proxyInfo {
	var pi proxyInfo
	s := &pi

	return s
}

func UnaryServerInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	resp, err = handler(ctx, req)
	log.Printf("%v -> %v", req, resp)
	return
}

func StreamServerInterceptor(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	err := handler(srv, stream)
	method := path.Base(info.FullMethod)
	log.Printf("stream %s %v", method, err)
	return err
}

func prepareGrpcServer(pi *proxyInfo, opts ...grpc.ServerOption) *grpc.Server {
	// we'd like to log the connection

	uIntOpt := grpc.UnaryInterceptor(UnaryServerInterceptor)
	sIntOpt := grpc.StreamInterceptor(StreamServerInterceptor)

	server := grpc.NewServer(uIntOpt, sIntOpt)
	api.RegisterSynerexServer(server, pi)
	return server
}

func providerInit(command nodeapi.KeepAliveCommand, ret string) {
	channelTypes := []uint32{uint32(*channel)}
	sxo := &sxutil.SxServerOpt{
		NodeType:  nodeapi.NodeType_PROVIDER,
		ClusterId: int32(*clusterId),
		AreaId:    "Default",
	}
	// set provider name with channel
	sname := fmt.Sprintf("%s:%d", *name, *channel)
	// obtain synerex server address from nodeserv
	srv, err := sxutil.RegisterNodeWithCmd(*nodesrv, sname, channelTypes, sxo, providerInit)
	if err != nil {
		log.Fatal("Can't register node...")
	}
	log.Printf("Connecting Server [%s]\n", srv)

	sxServerAddress = srv
	client := sxutil.GrpcConnectServer(srv)
	argJson := fmt.Sprintf("{Proxy:%d}", *channel)
	sclient = sxutil.NewSXServiceClient(client, uint32(*channel), argJson)
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

	proxyInfo := newProxyInfo()
	grpcServer := prepareGrpcServer(proxyInfo, opts...)
	log.Printf("Start Synerex Proxy Server[%s:%d], connection waiting at port :%d ...", *name, *channel, *port)
	serr := grpcServer.Serve(lis)
	log.Printf("Should not arrive here.. server closed. %v", serr)

	sxutil.CallDeferFunctions() // cleanup!

}
