package partition

// InitialAssignment distributes NSlots slots evenly across nodeIDs with
// replication factor rf. The algorithm:
//   - Divide [0, NSlots) into len(nodeIDs) equal bands.
//   - For slot i, the primary owner is nodeIDs[band(i)].
//   - The rf-1 co-replicas are the next rf-1 nodes in the list (wrapping).
//
// Example: 3 nodes [A,B,C], RF=2:
//
//	Slots 0–1365:    [A, B]
//	Slots 1366–2731: [B, C]
//	Slots 2732–4095: [C, A]
func InitialAssignment(nodeIDs []string, rf int) SlotMap {
	n := len(nodeIDs)
	if n == 0 {
		return empty()
	}
	if rf > n {
		rf = n
	}
	if rf < 1 {
		rf = 1
	}

	slots := make([][]string, NSlots)
	for i := range slots {
		primary := (i * n) / NSlots // band index [0, n)
		replicas := make([]string, rf)
		for r := 0; r < rf; r++ {
			replicas[r] = nodeIDs[(primary+r)%n]
		}
		slots[i] = replicas
	}
	return SlotMap{Version: 1, Slots: slots}
}

// RebalanceForJoin produces a new SlotMap where newNodeID takes over
// approximately NSlots/(currentNodeCount+1) slots from existing nodes.
//
// Strategy: every (n+1)th slot (0-indexed), replace one existing replica
// with the new node. The replaced replica is the one that appears most
// frequently as primary (slot[0]) to keep distribution even.
func RebalanceForJoin(current SlotMap, newNodeID string, rf int) SlotMap {
	if len(current.Slots) != NSlots {
		return current
	}
	n := countNodes(current)
	if rf > n+1 {
		rf = n + 1
	}

	// Build new slot list — deep copy first.
	slots := deepCopySlots(current.Slots)

	// Determine how many slots to give the new node.
	// Target: NSlots / (n+1), where n is the current node count.
	target := NSlots / (n + 1)
	assigned := 0

	// Walk slots in order; give every (n+1)th slot to the new node by
	// replacing its last replica (least-primary position).
	for i := range slots {
		if assigned >= target {
			break
		}
		// Skip slots that already have the new node.
		if containsID(slots[i], newNodeID) {
			continue
		}
		// Replace the last replica in this slot with the new node.
		if len(slots[i]) >= rf {
			slots[i][len(slots[i])-1] = newNodeID
		} else {
			slots[i] = append(slots[i], newNodeID)
		}
		assigned++
	}

	return SlotMap{Version: current.Version + 1, Slots: slots}
}

// RebalanceForLeave produces a new SlotMap where departingNodeID is removed
// from all slots and vacancies are filled with other nodes not already in
// that slot's replica list.
func RebalanceForLeave(current SlotMap, departingNodeID string, rf int) SlotMap {
	if len(current.Slots) != NSlots {
		return current
	}

	// Collect all known node IDs (excluding the departing one).
	nodeSet := make(map[string]struct{})
	for _, replicas := range current.Slots {
		for _, id := range replicas {
			if id != departingNodeID {
				nodeSet[id] = struct{}{}
			}
		}
	}
	remaining := make([]string, 0, len(nodeSet))
	for id := range nodeSet {
		remaining = append(remaining, id)
	}

	slots := deepCopySlots(current.Slots)
	for i, replicas := range slots {
		filtered := replicas[:0:len(replicas)]
		for _, id := range replicas {
			if id != departingNodeID {
				filtered = append(filtered, id)
			}
		}
		slots[i] = filtered

		// Fill vacancy if needed and possible.
		for len(slots[i]) < rf && len(remaining) > 0 {
			// Find a node not already in this slot.
			for _, candidate := range remaining {
				if !containsID(slots[i], candidate) {
					slots[i] = append(slots[i], candidate)
					break
				}
			}
			// Safety: if no candidate works, stop to avoid infinite loop.
			// (Can happen if remaining nodes < rf — data safety requires
			// adding nodes before removing, which is a deployment concern.)
			break
		}
	}

	return SlotMap{Version: current.Version + 1, Slots: slots}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func empty() SlotMap {
	slots := make([][]string, NSlots)
	for i := range slots {
		slots[i] = nil
	}
	return SlotMap{Version: 0, Slots: slots}
}

func countNodes(sm SlotMap) int {
	seen := make(map[string]struct{})
	for _, replicas := range sm.Slots {
		for _, id := range replicas {
			seen[id] = struct{}{}
		}
	}
	return len(seen)
}

func containsID(list []string, id string) bool {
	for _, s := range list {
		if s == id {
			return true
		}
	}
	return false
}

func deepCopySlots(src [][]string) [][]string {
	dst := make([][]string, len(src))
	for i, s := range src {
		cp := make([]string, len(s))
		copy(cp, s)
		dst[i] = cp
	}
	return dst
}
