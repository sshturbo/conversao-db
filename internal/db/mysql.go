package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"

	"conversao-db/internal/conversao"

	_ "github.com/go-sql-driver/mysql"
)

// OpenDB abre a conexão com o banco de dados MySQL
func OpenDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return db, nil
}

// EnviarParaMySQL insere os dados do JSON no banco de dados
func EnviarParaMySQL(jsonFile string, dsn string) error {
	file, err := os.Open(jsonFile)
	if err != nil {
		return fmt.Errorf("erro ao abrir arquivo JSON: %v", err)
	}
	defer file.Close()

	var dbExport conversao.DatabaseExport
	dec := json.NewDecoder(file)
	if err := dec.Decode(&dbExport); err != nil {
		return fmt.Errorf("erro ao decodificar JSON: %v", err)
	}

	db, err := OpenDB(dsn)
	if err != nil {
		return fmt.Errorf("erro ao conectar ao MySQL: %v", err)
	}
	defer db.Close()

	// Inserir admin
	result, err := db.Exec(`INSERT INTO accounts (nome, contato, email, login, senha, recuperar_senha, byid, mainid, accesstoken, valorrevenda, valorusuario, nivel) VALUES ('Admin', '62999999999', 'admin@admin.com', 'admin', 'admin', NULL, 0, 0, 0, 0.00, 0.00, 3)`)
	if err != nil {
		return fmt.Errorf("erro ao inserir admin: %v", err)
	}
	adminID, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("erro ao obter id do admin: %v", err)
	}

	// Mapear logins para IDs para preencher byid corretamente
	loginToID := map[string]int64{"admin": adminID}

	// Inserir categorias
	for _, cat := range dbExport.Categorias {
		_, err := db.Exec(`INSERT INTO categorias (subid, nome) VALUES (?, ?)`, cat.SubID, cat.Nome)
		if err != nil {
			return fmt.Errorf("erro ao inserir categoria %s: %v", cat.Nome, err)
		}
	}

	// Inserir revendas em accounts e atribuidos
	for _, rev := range dbExport.Revendas {
		byid := adminID // padrão: admin é o dono
		if donoID, ok := loginToID[rev.Dono]; ok {
			byid = donoID
		}
		mainid := int64(conversao.GerarMainID())
		result, err := db.Exec(`INSERT INTO accounts (nome, contato, email, login, senha, recuperar_senha, byid, mainid, accesstoken, valorrevenda, valorusuario, nivel) VALUES (?, ?, ?, ?, ?, NULL, ?, ?, 0, 0, 0, 2)`,
			rev.Nome,
			rev.Contato,
			rev.Email,
			rev.Login,
			rev.Senha,
			byid,
			mainid,
		)
		if err != nil {
			return fmt.Errorf("erro ao inserir revenda %s: %v", rev.Login, err)
		}
		revendaID, err := result.LastInsertId()
		if err != nil {
			return fmt.Errorf("erro ao obter id da revenda %s: %v", rev.Login, err)
		}
		loginToID[rev.Login] = revendaID
		_, err = db.Exec(`INSERT INTO atribuidos (valor, categoriaid, userid, byid, limite, limitetest, tipo, expira, subrev, suspenso) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
			rev.Valor,
			rev.CategoriaID,
			revendaID,
			byid,
			rev.Limite,
			rev.Limite,
			rev.Tipo,
			rev.Expira,
			rev.Sub,
		)
		if err != nil {
			return fmt.Errorf("erro ao inserir atribuido para revenda %s: %v", rev.Login, err)
		}
	}

	// Inserir usuários em ssh_accounts
	for _, user := range dbExport.Usuarios {
		// Buscar o id do dono na tabela accounts
		var donoID int64 = 0
		if id, ok := loginToID[user.Dono]; ok {
			donoID = id
		}
		_, err := db.Exec(`INSERT INTO ssh_accounts (login, senha, nome, expira, categoriaid, limite, contato, uuid, nivel, byid, mainid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)`,
			user.Login,
			user.Senha,
			user.Nome,
			user.Expira,
			user.CategoriaID,
			user.Limite,
			user.Contato,
			user.UUID,
			donoID,
			donoID,
		)
		if err != nil {
			return fmt.Errorf("erro ao inserir usuario %s em ssh_accounts: %v", user.Login, err)
		}
	}
	return nil
}
