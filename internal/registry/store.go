package registry

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	_ "modernc.org/sqlite" // driver "sqlite"

	"github.com/djalmajr/herdr-hermes/internal/outbox"
)

// Store is one open registry database. All access goes through a single
// connection (SetMaxOpenConns(1)), so every in-process transaction is
// BEGIN IMMEDIATE and serialized inside the process; a second process on
// the same file is excluded the same way and waits out busy_timeout.
type Store struct {
	db           *sql.DB
	now          func() time.Time
	eventsPerJob int
	onlineWindow time.Duration
}

// ErrUnsafeState: the database or one of its state files is a symlink or
// non-regular file, or (outside Windows) carries group or other mode bits,
// or its parent directory allows group or other writes.
var ErrUnsafeState = errors.New("registry: unsafe state file")

// schemaDDL is the v1 schema, created in one transaction. Every *_unix
// column is the Unix seconds of its text column; comparisons use the unix
// columns, the text columns are what the API returns.
const schemaDDL = `
CREATE TABLE nodes(maquina TEXT PRIMARY KEY, ultimo_contato TEXT, ultimo_contato_unix INTEGER, ultimo_poll TEXT, poll_status TEXT, herdr_version TEXT, soho_version TEXT, runners TEXT, manutencao INTEGER NOT NULL DEFAULT 0, manutencao_motivo TEXT);
CREATE TABLE jobs(maquina TEXT NOT NULL, chave TEXT NOT NULL, origem TEXT NOT NULL, job_id TEXT, workspace TEXT, workspace_id TEXT, agente TEXT, agente_fonte TEXT, trabalho TEXT, ref TEXT, card TEXT, projeto TEXT, branch TEXT, pr TEXT, fase TEXT, herdr_status TEXT, terminal INTEGER NOT NULL DEFAULT 0, saida INTEGER, esperando_humano INTEGER NOT NULL DEFAULT 0, espera_motivo TEXT, ultimo_seq INTEGER NOT NULL DEFAULT 0, ultima_atividade TEXT, ultima_atividade_unix INTEGER, obsoleto INTEGER NOT NULL DEFAULT 0, fonte_push INTEGER NOT NULL DEFAULT 0, fonte_poll INTEGER NOT NULL DEFAULT 0, visto_poll TEXT, sessao_ts_unix INTEGER, sessao_seq INTEGER, encerrado INTEGER NOT NULL DEFAULT 0, encerrado_unix INTEGER, PRIMARY KEY(maquina, chave));
CREATE TABLE events(maquina TEXT NOT NULL, chave TEXT NOT NULL, seq INTEGER NOT NULL, ts TEXT NOT NULL, tipo TEXT NOT NULL, resumo TEXT NOT NULL, refs TEXT, PRIMARY KEY(maquina, chave, seq));
CREATE TABLE waits(maquina TEXT NOT NULL, pane TEXT NOT NULL, workspace_id TEXT, agente TEXT, desde TEXT NOT NULL, desde_unix INTEGER NOT NULL, PRIMARY KEY(maquina, pane));
CREATE TABLE claims(id TEXT PRIMARY KEY, agente TEXT NOT NULL, maquina TEXT, card TEXT, projeto TEXT, branch TEXT, pr TEXT, workspace TEXT, ref TEXT, criado TEXT NOT NULL, criado_unix INTEGER NOT NULL, expira TEXT NOT NULL, expira_unix INTEGER NOT NULL);
CREATE TABLE policies(escopo TEXT NOT NULL, valor TEXT NOT NULL, hold INTEGER NOT NULL, prioridade INTEGER, motivo TEXT, atualizado TEXT NOT NULL, PRIMARY KEY(escopo, valor));
CREATE TABLE folders(maquina TEXT NOT NULL, caminho TEXT NOT NULL, tamanho_bytes INTEGER, pedido TEXT NOT NULL, job TEXT, criado TEXT NOT NULL, PRIMARY KEY(maquina, caminho));
`

// dsnFor builds the file: URI of the path, percent-escaped per segment so
// a space or a ? in the path survives, with the fixed pragma set and the
// immediate transaction lock.
func dsnFor(path string) string {
	segments := strings.Split(filepath.ToSlash(path), "/")
	for i, seg := range segments {
		segments[i] = url.PathEscape(seg)
	}
	return "file:" + strings.Join(segments, "/") +
		"?_pragma=busy_timeout(10000)" +
		"&_pragma=journal_mode(WAL)" +
		"&_pragma=foreign_keys(ON)" +
		"&_pragma=synchronous(FULL)" +
		"&_txlock=immediate"
}

// Open opens (creating the parent dir, the file and the schema when
// missing) the registry database at path. A database written by a newer
// schema is refused with ErrSchema, and a database or sidecar that is a
// symlink or non-regular file, that carries group or other mode bits,
// or that sits in a parent directory writable by group or other is
// refused with ErrUnsafeState.
func Open(path string, opts Options) (*Store, error) {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	eventsPerJob := opts.EventsPerJob
	if eventsPerJob <= 0 {
		eventsPerJob = DefaultEventsPerJob
	}
	onlineWindow := opts.OnlineWindow
	if onlineWindow <= 0 {
		onlineWindow = DefaultOnlineWindow
	}

	if err := unsafeStateCheck(path); err != nil {
		return nil, err
	}
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, err
		}
		// What MkdirAll created, or found created by someone else in the
		// meantime, is checked like the rest of the path.
		if err := unsafeStateCheck(path); err != nil {
			return nil, err
		}
	}
	// Pre-create the file at 0600 so the WAL and SHM files SQLite opens
	// alongside it inherit the same mode.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	// Recheck the state after the pre-create, and verify that the file just
	// opened is still the one at the path; a swap between the check and
	// the open refuses the open before SQLite touches anything.
	if err := unsafeStateCheck(path); err != nil {
		f.Close()
		return nil, err
	}
	fiOpen, errO := f.Stat()
	fiPath, errP := os.Lstat(path)
	if errO != nil || errP != nil || !os.SameFile(fiOpen, fiPath) {
		f.Close()
		return nil, ErrUnsafeState
	}
	f.Close()

	db, err := sql.Open("sqlite", dsnFor(path))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)

	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		db.Close()
		return nil, err
	}
	if version > SchemaVersion {
		db.Close()
		return nil, fmt.Errorf("%w: user_version %d", ErrSchema, version)
	}
	if err := migrate(db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db, now: now, eventsPerJob: eventsPerJob, onlineWindow: onlineWindow}, nil
}

// migrations[v] brings a version v schema to version v+1.
var migrations = map[int]func(*sql.Tx) error{
	1: func(tx *sql.Tx) error { return addColumn(tx, "jobs", "sessao_seq", "INTEGER") },
}

// addColumn adds a column unless the table already has it: one schema 1
// layout was written with it.
func addColumn(tx *sql.Tx, table, column, typ string) error {
	var n int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?`, table, column).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	_, err := tx.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column, typ))
	return err
}

// migrate creates the whole schema on a fresh database, or upgrades an
// older one step by step and bumps the version, then repairs the session
// order, in one immediate transaction on every open. Another process may
// have done it between the caller's read and the BEGIN, so the version is
// read again under the write lock.
func migrate(db *sql.DB) error {
	txn, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer txn.Rollback()
	var version int
	if err := txn.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return err
	}
	switch {
	case version > SchemaVersion:
		return fmt.Errorf("%w: user_version %d", ErrSchema, version)
	case version == SchemaVersion:
	case version == 0:
		if _, err := txn.Exec(schemaDDL); err != nil {
			return err
		}
	default:
		for v := version; v < SchemaVersion; v++ {
			if err := migrations[v](txn); err != nil {
				return fmt.Errorf("registry: migrate schema %d to %d: %w", v, v+1, err)
			}
		}
	}
	if version != SchemaVersion {
		if _, err := txn.Exec(fmt.Sprintf("PRAGMA user_version = %d", SchemaVersion)); err != nil {
			return err
		}
	}
	// Every write stores a session's outbox seq with its second, so a
	// session without one was stored before the seq was kept (schema 1,
	// or an early schema 2 upgrade): its seq is unknown, and no record of
	// its second may replace it, only a later one.
	if _, err := txn.Exec(`UPDATE jobs SET sessao_seq = ? WHERE sessao_ts_unix IS NOT NULL AND sessao_seq IS NULL`, int64(math.MaxInt64)); err != nil {
		return err
	}
	return txn.Commit()
}

// Close closes the database.
func (s *Store) Close() error {
	return s.db.Close()
}

// tx runs fn in one transaction, committing when fn returns nil and
// rolling back otherwise. Every mutation goes through it; the DSN's
// _txlock=immediate makes the BEGIN immediate.
func (s *Store) tx(fn func(*sql.Tx) error) error {
	txn, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	err = fn(txn)
	if err != nil {
		txn.Rollback()
		return err
	}
	return txn.Commit()
}

// admitNewNode refuses a node that would be the MaxNodes+1-th one.
func admitNewNode(tx *sql.Tx, maquina string) error {
	var exists int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM nodes WHERE maquina = ?`, maquina).Scan(&exists); err != nil {
		return err
	}
	if exists > 0 {
		return nil
	}
	var total int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM nodes`).Scan(&total); err != nil {
		return err
	}
	if total >= MaxNodes {
		return invalidErr("nodes", "limit reached")
	}
	return nil
}

// admitNewLiveJob refuses a live job row that would be the MaxJobs+1-th
// one. Polls are not capped.
func admitNewLiveJob(tx *sql.Tx) error {
	var live int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM jobs WHERE encerrado = 0`).Scan(&live); err != nil {
		return err
	}
	if live >= MaxJobs {
		return invalidErr("jobs", "limit reached")
	}
	return nil
}

// admitNewClaim refuses a claim that would be the MaxClaims+1-th live
// one, called after the expired claims are deleted.
func admitNewClaim(tx *sql.Tx) error {
	var live int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM claims`).Scan(&live); err != nil {
		return err
	}
	if live >= MaxClaims {
		return invalidErr("claims", "limit reached")
	}
	return nil
}

// admitNewPolicy refuses a policy that would be the MaxPolicies+1-th one;
// an update of an existing policy is not capped.
func admitNewPolicy(tx *sql.Tx, escopo, valor string) error {
	var exists int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM policies WHERE escopo = ? AND valor = ?`, escopo, valor).Scan(&exists); err != nil {
		return err
	}
	if exists > 0 {
		return nil
	}
	var total int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM policies`).Scan(&total); err != nil {
		return err
	}
	if total >= MaxPolicies {
		return invalidErr("policies", "limit reached")
	}
	return nil
}

// admitNewFolder refuses a folder record that would be the MaxFolders+1-th
// one; an update of an existing record is not capped.
func admitNewFolder(tx *sql.Tx, maquina, caminho string) error {
	var exists int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM folders WHERE maquina = ? AND caminho = ?`, maquina, caminho).Scan(&exists); err != nil {
		return err
	}
	if exists > 0 {
		return nil
	}
	var total int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM folders`).Scan(&total); err != nil {
		return err
	}
	if total >= MaxFolders {
		return invalidErr("folders", "limit reached")
	}
	return nil
}

// stamp is the store clock as a TSLayout string in the clock's own
// location (always a numeric offset, never Z) and its Unix seconds: the
// text column and its *_unix companion of one instant.
func (s *Store) stamp() (string, int64) {
	t := s.now()
	return t.Format(outbox.TSLayout), t.Unix()
}

// unsafeStateCheck runs before SQLite touches the path: the database and
// each existing sidecar must be regular files, and outside Windows none
// of them may carry group or other mode bits, nor may the parent
// directory allow group or other writes. The errors name the file and
// the reason, never its content.
func unsafeStateCheck(path string) error {
	type state struct {
		path string
		fi   os.FileInfo
	}
	var existing []state
	for _, p := range []string{path, path + "-wal", path + "-shm", path + "-journal"} {
		st, err := os.Lstat(p)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return err
		}
		if st.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: %s is a symlink", ErrUnsafeState, p)
		}
		if !st.Mode().IsRegular() {
			return fmt.Errorf("%w: %s is not a regular file", ErrUnsafeState, p)
		}
		existing = append(existing, state{path: p, fi: st})
	}
	if runtime.GOOS != "windows" {
		for _, st := range existing {
			if perm := st.fi.Mode().Perm() & 0o077; perm != 0 {
				return fmt.Errorf("%w: %s has group or other mode bits (%o)", ErrUnsafeState, st.path, perm)
			}
		}
		for _, st := range existing {
			if !ownedBySelfOrRoot(st.fi) {
				return fmt.Errorf("%w: %s is owned by another user", ErrUnsafeState, st.path)
			}
		}
		if err := checkAncestors(filepath.Dir(path)); err != nil {
			return err
		}
	}
	return nil
}

// checkAncestors walks every existing directory from the root down to
// dir, so nobody but the current user (or root) can swap an entry on the
// way to the database. A directory another user owns, or one writable by
// group or other, is refused; the one exception is a sticky shared
// directory such as /tmp, where only an entry's owner may replace it: the
// entry below it must then be a real directory owned by the current user
// or root, never a symlink. A missing part of the path ends the walk once
// its parent passed: Open creates it at 0700 and checks the path again.
// On top of the lexical walk, the resolved chain behind every alias is
// checked with the same rules, so a private alias cannot hide an unsafe
// real ancestor.
func checkAncestors(dir string) error {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	vol := filepath.VolumeName(abs)
	cur := vol + string(filepath.Separator)
	for _, part := range strings.Split(strings.TrimPrefix(abs[len(vol):], string(filepath.Separator)), string(filepath.Separator)) {
		if part == "" {
			continue
		}
		parent, err := os.Stat(cur)
		if err != nil {
			return err
		}
		if !ownedBySelfOrRoot(parent) {
			return fmt.Errorf("%w: %s is owned by another user", ErrUnsafeState, cur)
		}
		shared := parent.Mode().Perm()&0o022 != 0
		if shared && parent.Mode()&os.ModeSticky == 0 {
			return fmt.Errorf("%w: %s allows group or other writes", ErrUnsafeState, cur)
		}
		next := filepath.Join(cur, part)
		entry, err := os.Lstat(next)
		if errors.Is(err, os.ErrNotExist) {
			// Open creates the missing part at 0700 and checks the path
			// again; the walk stops once the parent passed.
			return checkResolvedAncestors(abs)
		}
		if err != nil {
			return err
		}
		if shared {
			if err := realOwnedDir(entry); err != nil {
				return fmt.Errorf("%w: %s in a shared directory is not a directory of the current user", ErrUnsafeState, next)
			}
		}
		cur = next
	}
	final, err := os.Stat(cur)
	if errors.Is(err, os.ErrNotExist) {
		return checkResolvedAncestors(abs)
	}
	if err != nil {
		return err
	}
	if !ownedBySelfOrRoot(final) {
		return fmt.Errorf("%w: %s is owned by another user", ErrUnsafeState, cur)
	}
	if final.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("%w: %s allows group or other writes", ErrUnsafeState, cur)
	}
	return checkResolvedAncestors(abs)
}

// realOwnedDir reports whether fi is a real directory (never a symlink)
// owned by the current user or root: the rule of the entry below a sticky
// shared directory.
func realOwnedDir(fi os.FileInfo) error {
	if fi.Mode()&os.ModeSymlink != 0 || !fi.Mode().IsDir() {
		return fmt.Errorf("%w: not a directory", ErrUnsafeState)
	}
	if !ownedBySelfOrRoot(fi) {
		return fmt.Errorf("%w: not owned by the current user", ErrUnsafeState)
	}
	return nil
}

// checkResolvedAncestors applies the ancestor rules to the resolved chain
// behind the aliases of dir: the deepest existing prefix is resolved with
// filepath.EvalSymlinks, and every directory of it, the last included, is
// checked with the same rules as the lexical walk (owner self or root, no
// group or other writes unless sticky, a real directory of the current
// user below a sticky shared directory), so the real location of an alias
// keeps its safety. The stricter rule of the final lexical directory
// still applies to the lexical path itself; a missing tail behind the
// prefix is created at 0700 and rechecked by Open.
func checkResolvedAncestors(abs string) error {
	prefix := abs
	for {
		if _, err := os.Lstat(prefix); !errors.Is(err, os.ErrNotExist) {
			break
		}
		prefix = filepath.Dir(prefix)
	}
	resolved, err := filepath.EvalSymlinks(prefix)
	if err != nil {
		return err
	}
	vol := filepath.VolumeName(resolved)
	cur := vol + string(filepath.Separator)
	for _, part := range strings.Split(strings.TrimPrefix(resolved[len(vol):], string(filepath.Separator)), string(filepath.Separator)) {
		if part == "" {
			continue
		}
		d, err := os.Lstat(cur)
		if err != nil {
			return err
		}
		if !ownedBySelfOrRoot(d) {
			return fmt.Errorf("%w: %s is owned by another user", ErrUnsafeState, cur)
		}
		if d.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: %s is a symlink", ErrUnsafeState, cur)
		}
		shared := d.Mode().Perm()&0o022 != 0
		if shared && d.Mode()&os.ModeSticky == 0 {
			return fmt.Errorf("%w: %s allows group or other writes", ErrUnsafeState, cur)
		}
		next := filepath.Join(cur, part)
		if shared {
			fi, err := os.Lstat(next)
			if err != nil {
				return err
			}
			if err := realOwnedDir(fi); err != nil {
				return fmt.Errorf("%w: %s in a shared directory is not a directory of the current user", ErrUnsafeState, next)
			}
		}
		cur = next
	}
	final, err := os.Lstat(cur)
	if err != nil {
		return err
	}
	if !ownedBySelfOrRoot(final) {
		return fmt.Errorf("%w: %s is owned by another user", ErrUnsafeState, cur)
	}
	if final.Mode().Perm()&0o022 != 0 && final.Mode()&os.ModeSticky == 0 {
		return fmt.Errorf("%w: %s allows group or other writes", ErrUnsafeState, cur)
	}
	return nil
}
