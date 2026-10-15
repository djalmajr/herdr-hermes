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
//	stop, cleanup := proctree.ForwardSignals(cmd) // around Wait, while the child runs
//	defer cleanup() // stops the relay's reception (the no-signal path only)
//	cmd.Cancel = ... proctree.Terminate(cmd) ... // on the context cancel
//	cmd.Wait()
//	sig, had := stop()
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
// hands it to the child's group while the child runs, and the caller
// ends on the very signal (ReRaise) when one was received. A run that
// received no signal changes nothing: the exit, the interrupted flag
// and the deadline are all decided as before. On Windows the relay is
// inert: the child never leaves the console group, so the console's
// Ctrl-C keeps reaching it.
package proctree
