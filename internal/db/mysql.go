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

// LimparTabelas remove todos os registros das tabelas
func LimparTabelas(db *sql.DB) error {
	// Desabilitar verificação de chave estrangeira temporariamente
	_, err := db.Exec("SET FOREIGN_KEY_CHECKS = 0")
	if err != nil {
		return fmt.Errorf("erro ao desabilitar foreign key checks: %v", err)
	}

	// Lista de tabelas para limpar
	tabelas := []string{"ssh_accounts", "atribuidos", "accounts", "categorias"}

	// Limpar cada tabela
	for _, tabela := range tabelas {
		_, err := db.Exec(fmt.Sprintf("TRUNCATE TABLE %s", tabela))
		if err != nil {
			return fmt.Errorf("erro ao limpar tabela %s: %v", tabela, err)
		}
	}

	// Reabilitar verificação de chave estrangeira
	_, err = db.Exec("SET FOREIGN_KEY_CHECKS = 1")
	if err != nil {
		return fmt.Errorf("erro ao reabilitar foreign key checks: %v", err)
	}

	return nil
}

// EnviarParaMySQL insere os dados diretamente no banco de dados
func EnviarParaMySQL(dbExport *conversao.DatabaseExport, dsn string) error {
	// Primeiro conectar sem especificar o banco para poder criá-lo
	dsnBase := strings.Split(dsn, "/")[0] + "/"
	db, err := OpenDB(dsnBase)
	if err != nil {
		return fmt.Errorf("erro ao conectar ao MySQL: %v", err)
	}

	// Extrair nome do banco da DSN
	dbName := strings.Split(strings.Split(dsn, "/")[1], "?")[0]

	// Criar o banco de dados com collation compatível
	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci", dbName))
	if err != nil {
		db.Close()
		return fmt.Errorf("erro ao criar banco de dados: %v", err)
	}
	db.Close()

	// Agora conectar ao banco específico
	db, err = OpenDB(dsn)
	if err != nil {
		return fmt.Errorf("erro ao conectar ao banco %s: %v", dbName, err)
	}
	defer db.Close()

	// Limpar tabelas existentes antes de inserir novos dados
	err = LimparTabelas(db)
	if err != nil {
		return fmt.Errorf("erro ao limpar tabelas: %v", err)
	}

	// Criar tabelas necessárias
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS accounts (
		id INT PRIMARY KEY AUTO_INCREMENT,
		nome VARCHAR(255),
		contato VARCHAR(255),
		email VARCHAR(255),
		login VARCHAR(255),
		senha VARCHAR(255),
		recuperar_senha VARCHAR(255),
		byid INT,
		mainid INT,
		accesstoken INT,
		valorrevenda DECIMAL(10,2),
		valorusuario DECIMAL(10,2),
		nivel INT
	) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci`)
	if err != nil {
		return fmt.Errorf("erro ao criar tabela accounts: %v", err)
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS categorias (
		id INT PRIMARY KEY AUTO_INCREMENT,
		subid INT,
		nome VARCHAR(255)
	) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci`)
	if err != nil {
		return fmt.Errorf("erro ao criar tabela categorias: %v", err)
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS ssh_accounts (
		id INT PRIMARY KEY AUTO_INCREMENT,
		login VARCHAR(255),
		senha VARCHAR(255),
		nome VARCHAR(255),
		expira DATETIME,
		categoriaid INT,
		limite INT,
		contato VARCHAR(255),
		uuid VARCHAR(255),
		nivel INT,
		byid INT,
		mainid INT
	) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci`)
	if err != nil {
		return fmt.Errorf("erro ao criar tabela ssh_accounts: %v", err)
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS atribuidos (
		id INT PRIMARY KEY AUTO_INCREMENT,
		valor DECIMAL(10,2),
		categoriaid INT,
		userid INT,
		byid INT,
		limite INT,
		limitetest INT,
		tipo VARCHAR(255),
		expira DATETIME,
		subrev INT,
		suspenso INT
	) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci`)
	if err != nil {
		return fmt.Errorf("erro ao criar tabela atribuidos: %v", err)
	}

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
		loginToMainID[rev.Login] = mainid

		_, err = db.Exec(`INSERT INTO atribuidos (valor, categoriaid, userid, byid, limite, limitetest, tipo, expira, subrev, suspenso) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
			rev.Valor,
			rev.CategoriaID,
			revendaID,
			byid,
			rev.Limite,
			rev.Limite,
			strings.Title(strings.TrimSpace(strings.ToLower(rev.Tipo))), // Remove espaços e garante primeira letra maiúscula
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
