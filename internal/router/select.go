package router

// Select applies the routing policy to the probed candidates, which must
// be in configured order. It is pure: no I/O, no time, no randomness.
//
// An available requested machine wins, even when another machine has
// fewer orchestrators (MotivoRequested). Otherwise the available machine
// with the fewest orchestrators wins, ties broken by the configured
// order (MotivoLeastLoad, or MotivoFallback when a requested machine
// existed but was not available). A requested label that is not
// configured is ErrUnknownRequested; no available machine is
// ErrNoneAvailable. In both error cases the result still carries the
// requested label and every candidate.
//
// Result.Candidates is always a copy of the given slice, same order, so
// callers may mutate either side without affecting the other.
func Select(cands []Candidate, requested string) (Result, error) {
	res := Result{Requested: requested}
	res.Candidates = append([]Candidate(nil), cands...)

	if requested != "" {
		i := -1
		for j := range cands {
			if cands[j].Machine == requested {
				i = j
				break
			}
		}
		if i < 0 {
			return res, ErrUnknownRequested
		}
		if isAvailable(cands[i]) {
			res.Machine = requested
			res.Motivo = MotivoRequested
			res.Orchestrators = *cands[i].Orchestrators
			return res, nil
		}
		return chooseAvailable(cands, res, MotivoFallback)
	}
	return chooseAvailable(cands, res, MotivoLeastLoad)
}

// chooseAvailable fills res with the available candidate that has the
// fewest orchestrators (a tie keeps the first in configured order), or
// reports ErrNoneAvailable with res carrying no machine.
func chooseAvailable(cands []Candidate, res Result, motivo string) (Result, error) {
	chosen := -1
	for i := range cands {
		if !isAvailable(cands[i]) {
			continue
		}
		if chosen < 0 || *cands[i].Orchestrators < *cands[chosen].Orchestrators {
			chosen = i
		}
	}
	if chosen < 0 {
		return res, ErrNoneAvailable
	}
	res.Machine = cands[chosen].Machine
	res.Motivo = motivo
	res.Orchestrators = *cands[chosen].Orchestrators
	return res, nil
}

// isAvailable reports whether c can be chosen: the state is available and
// the load was actually established. An available candidate whose count
// is nil is not available: a missing load is never inferred as zero.
func isAvailable(c Candidate) bool {
	return c.State == StateAvailable && c.Orchestrators != nil
}
