package db

import (
	"database/sql"
	"fmt"
	"strings"

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

// EnviarParaMySQL insere os dados diretamente no banco de dados
func EnviarParaMySQL(dbExport *conversao.DatabaseExport, dsn string) error {
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
	loginToMainID := map[string]int64{"admin": 0}

	// Inserir categorias
	for _, cat := range dbExport.Categorias {
		_, err := db.Exec(`INSERT INTO categorias (subid, nome) VALUES (?, ?)`, cat.SubID, cat.Nome)
		if err != nil {
			return fmt.Errorf("erro ao inserir categoria %s: %v", cat.Nome, err)
		}
	}

	// Inserir revendas em accounts e atribuidos
	for _, rev := range dbExport.Revendas {
		byid := adminID                          // padrão: admin é o dono
		mainid := int64(conversao.GerarMainID()) // Sempre gera um novo hash para cada revenda

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
		loginToMainID[rev.Login] = mainid // Salva o mainid único da revenda para herança dos usuários

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
		var mainid int64 = 0
		if id, ok := loginToID[user.Dono]; ok {
			donoID = id
		}
		if mid, ok := loginToMainID[user.Dono]; ok {
			mainid = mid // herda o mainid da revenda
		} else {
			mainid = int64(conversao.GerarMainID()) // fallback se não encontrar dono
		}

		nome := user.Nome
		if strings.TrimSpace(nome) == "" {
			nome = user.Login
		}

		uuid := user.UUID
		if strings.TrimSpace(uuid) == "" || uuid == "0" {
			uuid = "NULL"
		}

		query := `INSERT INTO ssh_accounts (login, senha, nome, expira, categoriaid, limite, contato, uuid, nivel, byid, mainid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)`
		if uuid == "NULL" {
			query = `INSERT INTO ssh_accounts (login, senha, nome, expira, categoriaid, limite, contato, uuid, nivel, byid, mainid) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, 1, ?, ?)`
			_, err = db.Exec(query,
				user.Login,
				user.Senha,
				nome,
				user.Expira,
				user.CategoriaID,
				user.Limite,
				user.Contato,
				donoID,
				mainid,
			)
		} else {
			_, err = db.Exec(query,
				user.Login,
				user.Senha,
				nome,
				user.Expira,
				user.CategoriaID,
				user.Limite,
				user.Contato,
				uuid,
				donoID,
				mainid,
			)
		}
		if err != nil {
			return fmt.Errorf("erro ao inserir usuario %s em ssh_accounts: %v", user.Login, err)
		}
	}
	return nil
}
