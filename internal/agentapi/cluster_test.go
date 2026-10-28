package agentapi

import "testing"

func TestParseClusterNodes(t *testing.T) {
	text := "07c37dfeb235213a872192d90877d0cd55635b91 10.244.1.7:6380@16380,s-0-0.s-0-headless.ns.svc myself,master - 0 1426238317239 1 connected 0-5460 [5461->-e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca]\n" +
		"e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 10.244.2.9:6380@16380 master - 0 1426238316232 2 connected 5461-10922 12000\n" +
		"292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 10.244.3.4:6380@16380,s-0-1.s-0-headless.ns.svc slave 07c37dfeb235213a872192d90877d0cd55635b91 0 1426238318243 3 connected\n" +
		"6ec23923021cf3ffec47632106199cb7f496ce01 :0@0 master,fail?,noaddr - 1426238316232 0 0 disconnected\n" +
		"\n"
	nodes := ParseClusterNodes(text)
	if len(nodes) != 4 {
		t.Fatalf("%d nodes", len(nodes))
	}
	me := nodes[0]
	if !me.Has("myself") || !me.IsMaster() || me.IP != "10.244.1.7" || me.Port != 6380 || me.BusPort != 16380 ||
		me.Hostname != "s-0-0.s-0-headless.ns.svc" || len(me.Slots) != 1 || me.SlotCount() != 5461 {
		t.Fatalf("myself: %+v", me)
	}
	if nodes[1].SlotCount() != 5462+1 || nodes[1].Hostname != "" {
		t.Fatalf("second: %+v (%d)", nodes[1], nodes[1].SlotCount())
	}
	if nodes[2].MasterID != me.ID || nodes[2].IsMaster() {
		t.Fatalf("replica: %+v", nodes[2])
	}
	if !nodes[3].Failed() || !nodes[3].Has("noaddr") || nodes[3].IP != "" {
		t.Fatalf("failed: %+v", nodes[3])
	}

	st := ClusterStatusResponse{Nodes: nodes}
	if m, ok := st.Myself(); !ok || m.ID != me.ID {
		t.Fatal("Myself")
	}
	if _, ok := st.Node("nope"); ok {
		t.Fatal("Node found an unknown id")
	}
}

func TestParseSlotRange(t *testing.T) {
	for s, want := range map[string][2]int{"0-5460": {0, 5460}, "42": {42, 42}, "16383": {16383, 16383}} {
		lo, hi, ok := ParseSlotRange(s)
		if !ok || lo != want[0] || hi != want[1] {
			t.Errorf("%q: %d %d %v", s, lo, hi, ok)
		}
	}
	for _, s := range []string{"", "x", "5-2", "16384", "-1", "1-x"} {
		if _, _, ok := ParseSlotRange(s); ok {
			t.Errorf("%q accepted", s)
		}
	}
}
