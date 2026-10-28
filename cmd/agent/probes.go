package main

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/kividbio/kividb-operator/internal/respclient"
)

// preStopPoll is how often GET /prestop looks at the engine's role.
var preStopPoll = 250 * time.Millisecond

// handlePreStop is the preStop hook of both of the pod's containers. It
// returns once the engine may be stopped: at once unless it is a master
// with replicas attached; for such a master, once the operator has handed
// the role to one of them (it watches for the pod's deletion and does that
// on its own), or after PRESTOP_TIMEOUT_SECONDS.
//
// Without it, a master deleted by hand, by a node drain or an eviction was
// stopped as the master: it came back from its shutdown snapshot, still the
// master, and its replicas -- which held every acknowledged write -- were
// resynced from that older snapshot. The agent container has the hook too,
// so that it is still there to answer while the kividb container waits:
// the kubelet stops both containers at once.
func (s *server) handlePreStop(w http.ResponseWriter, r *http.Request) {
	deadline := time.Now().Add(s.cfg.PreStopTimeout)
	logged := false
	for {
		hold, why := s.holdForHandover()
		if !hold {
			if logged {
				log.Printf("prestop: %s; letting kividb stop", why)
			}
			break
		}
		if !logged {
			log.Printf("prestop: %s; waiting up to %s for the operator to hand the master role over", why, s.cfg.PreStopTimeout)
			logged = true
		}
		if time.Now().After(deadline) {
			log.Printf("prestop: still the master after %s; letting kividb stop anyway", s.cfg.PreStopTimeout)
			break
		}
		select {
		case <-r.Context().Done():
			return
		case <-time.After(preStopPoll):
		}
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// holdForHandover reports whether the engine is a master with replicas
// attached, i.e. whether stopping it now would stop the master while some
// pod could have taken over.
func (s *server) holdForHandover() (bool, string) {
	c, err := s.dial()
	if err != nil {
		return false, "kividb does not answer"
	}
	defer c.Close()
	info, err := c.Info("replication")
	if err != nil {
		return false, "kividb does not report its role"
	}
	fields := respclient.ParseInfo(info)
	if fields["role"] != "master" {
		return false, "kividb is a replica"
	}
	n, _ := strconv.Atoi(fields["connected_slaves"])
	if n == 0 {
		return false, "kividb is a master with no replica attached"
	}
	return true, fmt.Sprintf("kividb is a master with %d replica(s) attached", n)
}
