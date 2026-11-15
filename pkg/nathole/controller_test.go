package nathole

import (
	"testing"
	"time"

	"github.com/fatedier/frp/pkg/msg"
	"github.com/fatedier/frp/pkg/transport"
)

type visitorResponseRecorder struct {
	response msg.Message
}

func (r *visitorResponseRecorder) Send(response msg.Message) error {
	r.response = response
	return nil
}

func TestHandleVisitorDeniesUsersOutsideProxyPolicy(t *testing.T) {
	controller, err := NewController(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.ListenClient("private", "test-secret", []string{"allowed"}); err != nil {
		t.Fatal(err)
	}

	for _, preCheck := range []bool{true, false} {
		recorder := &visitorResponseRecorder{}
		controller.HandleVisitor(&msg.NatHoleVisitor{
			ProxyName: "private",
			PreCheck:  preCheck,
		}, transport.NewMessageTransporter(recorder), "excluded")
		response, ok := recorder.response.(*msg.NatHoleResp)
		if !ok || response.Error != "xtcp visitor user [excluded] not allowed for [private]" || response.Sid != "" {
			t.Fatalf("preCheck=%v: expected policy rejection, got %#v", preCheck, recorder.response)
		}
	}
}
