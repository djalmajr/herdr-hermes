// Package proctree terminates the process tree of a subprocess when the
// subprocess is cancelled on its deadline. It owns the platform
// mechanism in one place: on unix the child is put in its own process
// group and the group is signalled; on Windows the child is assigned to
// a job object that is terminated on the cancel. A child that exits on
// its own is never touched: the descendants it left on purpose keep
// running, because a bounded command may leave legitimate work behind.
//
// The sequence a caller runs is:
//
//	proctree.Prepare(cmd)              // before cmd.Start
//	cmd.Start()
//	release, _ := proctree.Attach(cmd) // right after Start
//	defer release()
//	stop := proctree.ForwardSignals(cmd) // around Wait, while the child runs
//	cmd.Cancel = ... proctree.Terminate(cmd) ... // on the context cancel
//	cmd.Wait()
//	sig, had := stop() // ends the relay, stops the reception, reports the signal
//	if had { proctree.ReRaise(sig) }    // the run ends on the very signal
//	if the cancel fired { proctree.Kill(cmd) }   // after Wait
//
// Terminate is the graceful tree termination on the cancel, and Kill is
// the force the platform needs after Wait has returned from a
// cancelled run to end whatever the graceful step left behind. Neither
// is called on a run that finished without a cancel, so the group or
// job of a child that exited on its own is never touched.
//
// ForwardSignals and ReRaise keep an interrupted run from leaving the
// child's tree behind: the group isolation that makes the cancel reach
// the whole tree also takes the child out of the foreground group, so
// a terminal signal (the Ctrl-C) reaches only the caller; the relay
// hands it to the child's group while the child runs, and the first
// forwarded signal starts a fixed grace (the same wait the caller
// gives the cancel): a group that survives it is forced down so the
// wait returns, and the caller then ends on the very signal (ReRaise)
// when one was received. A run that received no signal changes nothing:
// the reception the relay registered is stopped by the stop, and the
// exit, the interrupted flag and the deadline are all decided as
// before. On Windows the relay is inert: the child never leaves the
// console group, so the console's Ctrl-C keeps reaching it.
package proctree

import "time"

// Grace is the one grace the tree gets before it is forced down: the
// WaitDelay the callers give the cancel and the wait the first
// forwarded terminal signal gives the child's group. The callers set
// their WaitDelay from it, so the two can never disagree.
const Grace = 5 * time.Second
