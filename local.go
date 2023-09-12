package main

type Node struct {
	Addr          string
	Name          string
	Weight        int
	CurrentWeight int
}

func next(ss []*Node) (best *Node) {
	total := 0
	best = ss[0]
	for i := 0; i < len(ss); i++ {
		w := ss[i]
		w.CurrentWeight += w.Weight
		total += w.Weight
		if w.CurrentWeight > best.CurrentWeight {
			best = w
		}
	}
	best.CurrentWeight -= total
	return best
}
