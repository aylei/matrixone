package pythonservice

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"reflect"
	"sync"
	"testing"
)

func TestClient_Run(t *testing.T) {
	type fields struct {
		cfg   ClientConfig
		sc    udf.ServiceClient
		mutex sync.Mutex
	}
	type args struct {
		ctx       context.Context
		request   *udf.Request
		pkgReader udf.PkgReader
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *udf.Response
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				cfg:   tt.fields.cfg,
				sc:    tt.fields.sc,
				mutex: tt.fields.mutex,
			}
			got, err := c.Run(tt.args.ctx, tt.args.request, tt.args.pkgReader)
			if (err != nil) != tt.wantErr {
				t.Errorf("Run() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Run() got = %v, want %v", got, tt.want)
			}
		})
	}
}
