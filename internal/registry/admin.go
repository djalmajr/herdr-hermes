package registry

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

// Release deletes one unexpired claim. An unknown or expired id is
// ErrNotFound; an id that is not 32 lower-case hex is ErrInvalid.
func (s *Store) Release(id string) error {
	if !claimIDPattern.MatchString(id) {
		return invalidErr("claim id", "bad format")
	}
	return s.tx(func(tx *sql.Tx) error {
		res, err := tx.Exec(`DELETE FROM claims WHERE id = ? AND expira_unix > ?`, id, s.now().Unix())
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("%w: claim not found", ErrNotFound)
		}
		return nil
	})
}

// SetPolicy upserts one hold or priority, stamping it with the current
// time. A policy with hold = false and no priority records nothing.
func (s *Store) SetPolicy(in PolicyInput) error {
	escopo, valor, err := validatePolicyScope(in.Escopo, in.Valor)
	if err != nil {
		return err
	}
	if !in.Hold && in.Prioridade == nil {
		return invalidErr("policy", "nothing to record")
	}
	if in.Prioridade != nil && (*in.Prioridade < -1000 || *in.Prioridade > 1000) {
		return invalidErr("prioridade", "out of range")
	}
	if err := codePointLimit("motivo", in.Motivo, 280); err != nil {
		return err
	}
	return s.tx(func(tx *sql.Tx) error {
		if err := admitNewPolicy(tx, escopo, valor); err != nil {
			return err
		}
		atualizado, _ := s.stamp()
		_, err := tx.Exec(`INSERT INTO policies(escopo, valor, hold, prioridade, motivo, atualizado) VALUES(?,?,?,?,?,?)
			ON CONFLICT(escopo, valor) DO UPDATE SET hold = excluded.hold, prioridade = excluded.prioridade, motivo = excluded.motivo, atualizado = excluded.atualizado`,
			escopo, valor, in.Hold, in.Prioridade, nullStr(in.Motivo), atualizado)
		return err
	})
}

// ClearPolicy deletes one policy or reports ErrNotFound.
func (s *Store) ClearPolicy(escopo, valor string) error {
	escopo, valor, err := validatePolicyScope(escopo, valor)
	if err != nil {
		return err
	}
	return s.tx(func(tx *sql.Tx) error {
		res, err := tx.Exec(`DELETE FROM policies WHERE escopo = ? AND valor = ?`, escopo, valor)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("%w: policy not found", ErrNotFound)
		}
		return nil
	})
}

// SetFolder upserts one folder record, keeping the original criado on an
// update. Nothing on disk is ever touched.
func (s *Store) SetFolder(in FolderInput) error {
	if err := validateMachine(in.Maquina); err != nil {
		return err
	}
	if err := validateFolder(in.Caminho, in.Pedido, in.TamanhoBytes); err != nil {
		return err
	}
	return s.tx(func(tx *sql.Tx) error {
		if err := admitNewFolder(tx, in.Maquina, in.Caminho); err != nil {
			return err
		}
		criado, _ := s.stamp()
		_, err := tx.Exec(`INSERT INTO folders(maquina, caminho, tamanho_bytes, pedido, job, criado) VALUES(?,?,?,?,?,?)
			ON CONFLICT(maquina, caminho) DO UPDATE SET tamanho_bytes = excluded.tamanho_bytes, pedido = excluded.pedido, job = excluded.job`,
			in.Maquina, in.Caminho, in.TamanhoBytes, in.Pedido, nullStr(in.Job), criado)
		return err
	})
}

// ClearFolder deletes one folder record or reports ErrNotFound.
func (s *Store) ClearFolder(maquina, caminho string) error {
	if err := validateMachine(maquina); err != nil {
		return err
	}
	if err := validateFolderPath(caminho); err != nil {
		return err
	}
	return s.tx(func(tx *sql.Tx) error {
		res, err := tx.Exec(`DELETE FROM folders WHERE maquina = ? AND caminho = ?`, maquina, caminho)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("%w: folder not found", ErrNotFound)
		}
		return nil
	})
}

// SetNode upserts the node and changes only the non-nil fields; it never
// sets online or contact times. manutencao = false clears the stored
// manutencao_motivo unless the same update sets one explicitly.
func (s *Store) SetNode(u NodeUpdate) error {
	if err := validateMachine(u.Maquina); err != nil {
		return err
	}
	if u.SohoVersion != nil {
		if err := printableFact("soho_version", *u.SohoVersion, 120); err != nil {
			return err
		}
	}
	if u.ManutencaoMotivo != nil {
		if err := printableFact("manutencao_motivo", *u.ManutencaoMotivo, 280); err != nil {
			return err
		}
	}
	if u.Runners != nil {
		if err := validateRunners(u.Runners); err != nil {
			return err
		}
	}
	return s.tx(func(tx *sql.Tx) error {
		if err := admitNewNode(tx, u.Maquina); err != nil {
			return err
		}
		cols := []string{"maquina"}
		updates := make([]string, 0, 4)
		args := []any{u.Maquina}
		if u.Manutencao != nil {
			v := 0
			if *u.Manutencao {
				v = 1
			}
			cols = append(cols, "manutencao")
			args = append(args, v)
			updates = append(updates, "manutencao = excluded.manutencao")
		}
		if u.ManutencaoMotivo != nil {
			cols = append(cols, "manutencao_motivo")
			args = append(args, *u.ManutencaoMotivo)
			updates = append(updates, "manutencao_motivo = excluded.manutencao_motivo")
		} else if u.Manutencao != nil && !*u.Manutencao {
			updates = append(updates, "manutencao_motivo = NULL")
		}
		if u.SohoVersion != nil {
			cols = append(cols, "soho_version")
			args = append(args, *u.SohoVersion)
			updates = append(updates, "soho_version = excluded.soho_version")
		}
		if u.Runners != nil {
			b, err := json.Marshal(u.Runners)
			if err != nil {
				return err
			}
			cols = append(cols, "runners")
			args = append(args, string(b))
			updates = append(updates, "runners = excluded.runners")
		}
		stmt := "INSERT INTO nodes(" + strings.Join(cols, ",") + ") VALUES(" + strings.Repeat("?,", len(cols)-1) + "?)"
		if len(updates) > 0 {
			stmt += " ON CONFLICT(maquina) DO UPDATE SET " + strings.Join(updates, ", ")
		} else {
			stmt += " ON CONFLICT(maquina) DO NOTHING"
		}
		_, err := tx.Exec(stmt, args...)
		return err
	})
}
