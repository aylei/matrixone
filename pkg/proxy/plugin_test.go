// Copyright 2021 - 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"context"
	"testing"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plugin"
	"github.com/stretchr/testify/require"
)

var _ Router = (*pluginRouter)(nil)

type mockPlugin struct {
	mockRecommendCNFn func(ctx context.Context, clientInfo clientInfo) (*plugin.Recommendation, error)
}

func (p *mockPlugin) RecommendCN(ctx context.Context, clientInfo clientInfo) (*plugin.Recommendation, error) {
	if p.mockRecommendCNFn != nil {
		return p.mockRecommendCNFn(ctx, clientInfo)
	}
	return &plugin.Recommendation{
		Action: plugin.Bypass,
	}, nil
}

type mockRouter struct {
	mockRouteFn func(ctx context.Context, ci clientInfo) (*CNServer, error)
}

func (r *mockRouter) Route(ctx context.Context, ci clientInfo) (*CNServer, error) {
	if r.mockRouteFn != nil {
		return r.mockRouteFn(ctx, ci)
	}
	return nil, nil
}

func (r *mockRouter) SelectByConnID(connID uint32) (*CNServer, error) {
	return nil, nil
}

func (r *mockRouter) Connect(c *CNServer, handshakeResp *frontend.Packet, t *tunnel) (ServerConn, []byte, error) {
	return nil, nil, nil
}

func TestPluginRouter_Route(t *testing.T) {
	defer leaktest.AfterTest(t)()

	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	tests := []struct {
		name              string
		mockRouteFn       func(ctx context.Context, ci clientInfo) (*CNServer, error)
		mockRecommendCNFn func(ctx context.Context, ci clientInfo) (*plugin.Recommendation, error)
		expectErr         bool
		expectUUID        string
	}{{
		name: "recommend select CN",
		mockRecommendCNFn: func(ctx context.Context, ci clientInfo) (*plugin.Recommendation, error) {
			return &plugin.Recommendation{
				Action: plugin.Select,
				CN: &metadata.CNService{
					ServiceID: "cn0",
				},
			}, nil
		},
		expectUUID: "cn0",
	}, {
		name: "recommend bypass",
		mockRecommendCNFn: func(ctx context.Context, ci clientInfo) (*plugin.Recommendation, error) {
			return &plugin.Recommendation{
				Action: plugin.Bypass,
			}, nil
		},
		mockRouteFn: func(ctx context.Context, ci clientInfo) (*CNServer, error) {
			return &CNServer{uuid: "cn1"}, nil
		},
		expectUUID: "cn1",
	}, {
		name: "recommend reject",
		mockRecommendCNFn: func(ctx context.Context, ci clientInfo) (*plugin.Recommendation, error) {
			return &plugin.Recommendation{
				Action:  plugin.Reject,
				Message: "IP not in whitelist",
			}, nil
		},
		expectErr: true,
	}, {
		name: "error after bypass",
		mockRecommendCNFn: func(ctx context.Context, ci clientInfo) (*plugin.Recommendation, error) {
			return &plugin.Recommendation{
				Action: plugin.Bypass,
			}, nil
		},
		mockRouteFn: func(ctx context.Context, ci clientInfo) (*CNServer, error) {
			return nil, moerr.NewInternalErrorNoCtx("boom")
		},
		expectErr: true,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &mockPlugin{mockRecommendCNFn: tt.mockRecommendCNFn}
			r := &mockRouter{mockRouteFn: tt.mockRouteFn}
			pr := &pluginRouter{
				plugin: p,
				Router: r,
			}
			cn, err := pr.Route(context.TODO(), clientInfo{})
			if tt.expectErr {
				require.Error(t, err)
				require.Nil(t, cn)
			} else {
				require.NotNil(t, cn)
				require.Equal(t, cn.uuid, tt.expectUUID)
			}
		})
	}
}
