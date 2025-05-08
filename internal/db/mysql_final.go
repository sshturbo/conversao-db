package db

import (
	"database/sql"
	"fmt"
	"strings"

	"conversao-db/internal/conversao"
)

// EnviarParaMySQLFinal insere os dados no formato final para o MySQL
func EnviarParaMySQLFinal(dbFinal *conversao.DatabaseFinal, dsn string) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("erro ao conectar ao banco de dados: %v", err)
	}
	defer db.Close()

	// Inserir categorias
	for _, cat := range dbFinal.Categorias {
		_, err := db.Exec(`INSERT INTO categorias (id, subid, nome) VALUES (?, ?, ?)`,
			cat.ID,
			cat.SubID,
			strings.TrimSpace(cat.Nome),
		)
		if err != nil {
			return fmt.Errorf("erro ao inserir categoria %s: %v", cat.Nome, err)
		}
	}

	// Inserir accounts
	for _, acc := range dbFinal.Accounts {
		_, err := db.Exec(`INSERT INTO accounts (
			id, nome, contato, email, login, senha, byid, mainid, accesstoken, valorusuario, valorrevenda, nivel
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			acc.ID,
			strings.TrimSpace(acc.Nome),
			strings.TrimSpace(acc.Contato),
			strings.TrimSpace(acc.Email),
			strings.TrimSpace(acc.Login),
			strings.TrimSpace(acc.Senha),
			strings.TrimSpace(acc.ByID),
			strings.TrimSpace(acc.MainID),
			strings.TrimSpace(acc.AccessToken),
			strings.TrimSpace(acc.ValorUsuario),
			strings.TrimSpace(acc.ValorRevenda),
			acc.Nivel,
		)
		if err != nil {
			return fmt.Errorf("erro ao inserir account %s: %v", acc.Login, err)
		}
	}

	// Inserir ssh_accounts
	for _, ssh := range dbFinal.SSHAccounts {
		_, err := db.Exec(`INSERT INTO ssh_accounts (
			id, byid, categoriaid, limite, login, nome, senha, mainid, expira, uuid, contato
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			ssh.ID,
			ssh.ByID,
			ssh.CategoriaID,
			ssh.Limite,
			strings.TrimSpace(ssh.Login),
			strings.TrimSpace(ssh.Nome),
			strings.TrimSpace(ssh.Senha),
			strings.TrimSpace(ssh.MainID),
			strings.TrimSpace(ssh.Expira),
			strings.TrimSpace(ssh.UUID),
			strings.TrimSpace(ssh.Contato),
		)
		if err != nil {
			return fmt.Errorf("erro ao inserir ssh_account %s: %v", ssh.Login, err)
		}
	}

	// Inserir atribuidos
	for _, atr := range dbFinal.Atribuidos {
		_, err := db.Exec(`INSERT INTO atribuidos (
			id, valor, categoriaid, userid, byid,
			limite, limitetest, tipo, expira, subrev,
			suspenso
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			atr.ID,
			strings.TrimSpace(atr.Valor),
			atr.CategoriaID,
			atr.UserID,
			atr.ByID,
			atr.Limite,
			atr.LimiteTest,
			strings.TrimSpace(atr.Tipo),
			strings.TrimSpace(atr.Expira),
			atr.SubRev,
			atr.Suspenso,
		)
		if err != nil {
			return fmt.Errorf("erro ao inserir atribuido para usuário %d: %v", atr.UserID, err)
		}
	}

	return nil
}
