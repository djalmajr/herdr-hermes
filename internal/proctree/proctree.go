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
//	cmd.Cancel = ... proctree.Terminate(cmd) ... // on the context cancel
//	cmd.Wait()
//	if the cancel fired { proctree.Kill(cmd) }   // after Wait
//
// Terminate is the graceful tree termination on the cancel, and Kill is
// the force the platform needs after Wait has returned from a
// cancelled run to end whatever the graceful step left behind. Neither
// is called on a run that finished without a cancel, so the group or
// job of a child that exited on its own is never touched.
package proctree
