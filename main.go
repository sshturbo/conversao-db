package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"conversao-db/internal/db"

	"github.com/JamesStewy/go-mysqldump"
	_ "github.com/go-sql-driver/mysql"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/joho/godotenv"
)

type Categoria struct {
	ID    int    `json:"id"`
	SubID int    `json:"subid"`
	Nome  string `json:"nome"`
}

type Usuario struct {
	ID         int     `json:"id"`
	MainID     int     `json:"mainid"`
	SubID      int     `json:"subid"`
	Login      string  `json:"login"`
	Senha      string  `json:"senha"`
	Nome       string  `json:"nome"`
	Validade   string  `json:"validade"`
	Valor      float64 `json:"valor"`
	Notificado int     `json:"notificado"`
	Whatsapp   string  `json:"whatsapp"`
	UUID       string  `json:"uuid"`
	Status     int     `json:"status"`
	Limite     int     `json:"limite"`
	Suspenso   int     `json:"suspenso"`
	Periodo    int     `json:"periodo"`
	Teste      int     `json:"teste"`
	Telegram   string  `json:"telegram"`
}

type Revenda struct {
	ID         int     `json:"id"`
	MainID     int     `json:"mainid"`
	Login      string  `json:"login"`
	Senha      string  `json:"senha"`
	Numero     string  `json:"numero"`
	Valor      float64 `json:"valor"`
	Limite     int     `json:"limite"`
	Modo       string  `json:"modo"`
	Data       string  `json:"data"`
	LimiteUse  int     `json:"limite_use"`
	Categoria  int     `json:"categoria"`
	Sub        int     `json:"sub"`
	Expirado   int     `json:"expirado"`
	TextoRev   string  `json:"textorev"`
	TextoUser  string  `json:"textouser"`
	APIKey     string  `json:"apikey"`
	Notificado int     `json:"notificado"`
	TextoTeste string  `json:"texto_teste"`
	ValorTeste float64 `json:"valor_teste"`
	V2RayTeste int     `json:"v2ray_teste"`
}

// Estrutura que conterá todas as tabelas
type Database struct {
	Categorias []Categoria `json:"categorias"`
	Usuarios   []Usuario   `json:"usuarios"`
	Revendas   []Revenda   `json:"revendas"`
}

// Estruturas auxiliares para exportação com campos extras

type UsuarioExport struct {
	Login         string `json:"login"`
	Senha         string `json:"senha"`
	Nome          string `json:"nome"`
	Expira        string `json:"expira"`
	Suspenso      int    `json:"suspenso"`
	Dono          string `json:"dono"`
	CategoriaNome string `json:"categoria_nome"`
	Contato       string `json:"contato"`
	CategoriaID   int    `json:"categoriaid"`
	Limite        int    `json:"limite"`
	UUID          string `json:"uuid"`
}

type RevendaExport struct {
	Login         string  `json:"login"`
	Senha         string  `json:"senha"`
	Contato       string  `json:"contato"`
	Valor         float64 `json:"valor"`
	Limite        int     `json:"limite"`
	Tipo          string  `json:"tipo"`
	Expira        string  `json:"expira"`
	CategoriaID   int     `json:"categoriaid"`
	Sub           int     `json:"sub"`
	Dono          string  `json:"dono"`
	CategoriaNome string  `json:"categoria_nome"`
	Nome          string  `json:"nome"`
	Email         string  `json:"email"`
}

type DatabaseExport struct {
	Categorias []Categoria     `json:"categorias"`
	Usuarios   []UsuarioExport `json:"usuarios"`
	Revendas   []RevendaExport `json:"revendas"`
}

// Função para baixar arquivo de uma URL
func downloadFile(url string, filepath string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	return err
}

// Função principal de processamento do SQL para JSON
func processarArquivoSQL(inputFile, outputFile string) error {
	file, err := os.Open(inputFile)
	if err != nil {
		return fmt.Errorf("erro ao abrir arquivo: %v", err)
	}
	defer file.Close()

	var db Database

	scanner := bufio.NewScanner(file)
	inInsert := false
	currentTable := ""
	var values string

	for scanner.Scan() {
		line := scanner.Text()

		if strings.HasPrefix(line, "DROP TABLE") || strings.HasPrefix(line, "CREATE TABLE") {
			continue
		}

		if strings.Contains(line, "INSERT INTO `categorias`") || strings.HasPrefix(line, "INSERT INTO categorias VALUES") {
			inInsert = true
			currentTable = "categorias"
			values = ""
			if idx := strings.Index(line, "VALUES("); idx != -1 {
				val := line[idx+7:]
				val = strings.TrimSuffix(val, ";")
				val = strings.TrimSuffix(val, ")")
				val = strings.TrimPrefix(val, "(")
				fields := splitFields(val)
				cat := parseCategoria(fields)
				db.Categorias = append(db.Categorias, cat)
				inInsert = false
				continue
			}
			continue
		} else if strings.Contains(line, "INSERT INTO `revenda`") || strings.HasPrefix(line, "INSERT INTO revenda VALUES") {
			inInsert = true
			currentTable = "revenda"
			values = ""
			if idx := strings.Index(line, "VALUES("); idx != -1 {
				val := line[idx+7:]
				val = strings.TrimSuffix(val, ";")
				val = strings.TrimSuffix(val, ")")
				val = strings.TrimPrefix(val, "(")
				fields := splitFields(val)
				rev := parseRevenda(fields)
				db.Revendas = append(db.Revendas, rev)
				inInsert = false
				continue
			}
			continue
		} else if strings.Contains(line, "INSERT INTO `usuarios`") || strings.HasPrefix(line, "INSERT INTO usuarios VALUES") {
			inInsert = true
			currentTable = "usuarios"
			values = ""
			if idx := strings.Index(line, "VALUES("); idx != -1 {
				val := line[idx+7:]
				val = strings.TrimSuffix(val, ";")
				val = strings.TrimSuffix(val, ")")
				val = strings.TrimPrefix(val, "(")
				fields := splitFields(val)
				user := parseUsuario(fields)
				db.Usuarios = append(db.Usuarios, user)
				inInsert = false
				continue
			}
			continue
		}

		if inInsert && strings.HasPrefix(line, "(") {
			values += line
			if strings.HasSuffix(strings.TrimSpace(line), ";") {
				values = strings.TrimSuffix(strings.TrimSpace(values), ";")
				rows := strings.Split(values, "),(")
				for _, row := range rows {
					row = strings.Trim(row, "()")
					fields := splitFields(row)
					if currentTable == "categorias" {
						cat := parseCategoria(fields)
						db.Categorias = append(db.Categorias, cat)
					} else if currentTable == "revenda" {
						rev := parseRevenda(fields)
						db.Revendas = append(db.Revendas, rev)
					} else if currentTable == "usuarios" {
						user := parseUsuario(fields)
						db.Usuarios = append(db.Usuarios, user)
					}
				}
				inInsert = false
				values = ""
			}
		} else if inInsert {
			values += line
		}
	}

	// Monta os dados de exportação com os campos extras
	var dbExport DatabaseExport
	dbExport.Categorias = db.Categorias
	for _, user := range db.Usuarios {
		contato := strings.TrimSpace(user.Whatsapp)
		if contato == "" && len(user.Login) > 0 {
			contato = gerarContatoAleatorio()
		}
		expira := user.Validade
		if t, err := time.Parse("2006-01-02 15:04:05", user.Validade); err == nil {
			expira = t.Format("2006-01-02T15:04:05")
		}
		dbExport.Usuarios = append(dbExport.Usuarios, UsuarioExport{
			Login:         user.Login,
			Senha:         user.Senha,
			Nome:          user.Nome,
			Expira:        expira,
			Suspenso:      user.Suspenso,
			Dono:          getDonoUsuario(user, db),
			CategoriaNome: getNomeCategoriaPorSubID(user.SubID, db),
			Contato:       contato,
			CategoriaID:   user.SubID,
			Limite:        user.Limite,
			UUID:          user.UUID,
		})
	}
	for _, rev := range db.Revendas {
		dataFormatada := rev.Data
		if t, err := time.Parse("2006-01-02", rev.Data); err == nil {
			dataFormatada = t.Format("2006-01-02T15:04:05")
		}
		contato := strings.TrimSpace(rev.Numero)
		if contato == "" {
			contato = gerarContatoAleatorio()
		}
		email := strings.TrimSpace(rev.Login) + "@gmail.com"
		dbExport.Revendas = append(dbExport.Revendas, RevendaExport{
			Login:         rev.Login,
			Senha:         rev.Senha,
			Contato:       contato,
			Valor:         rev.Valor,
			Limite:        rev.Limite,
			Tipo:          rev.Modo,
			Expira:        dataFormatada,
			CategoriaID:   rev.Categoria,
			Sub:           rev.Sub,
			Dono:          getDonoRevenda(rev, db),
			CategoriaNome: getNomeCategoriaPorSubID(rev.Categoria, db),
			Nome:          rev.Login,
			Email:         email,
		})
	}

	jsonData, err := json.MarshalIndent(dbExport, "", "    ")
	if err != nil {
		return fmt.Errorf("erro ao converter para JSON: %v", err)
	}

	outFile, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("erro ao criar arquivo JSON: %v", err)
	}
	defer outFile.Close()
	_, err = outFile.Write(jsonData)
	if err != nil {
		return fmt.Errorf("erro ao salvar arquivo JSON: %v", err)
	}
	return nil
}

func main() {
	// Carregar variáveis do .env
	_ = godotenv.Load()

	telegramToken := os.Getenv("TELEGRAM_TOKEN")
	if telegramToken == "" {
		log.Fatal("TELEGRAM_TOKEN não definido no .env")
	}

	bot, err := tgbotapi.NewBotAPI(telegramToken)
	if err != nil {
		log.Panic(err)
	}
	bot.Debug = false
	log.Printf("Bot autorizado em %s", bot.Self.UserName)

	dbUser := os.Getenv("DB_USER")
	dbPass := os.Getenv("DB_PASS")
	dbHost := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	dbName := os.Getenv("DB_NAME")
	if dbUser == "" || dbPass == "" || dbHost == "" || dbPort == "" || dbName == "" {
		log.Fatal("Dados de conexão do banco não definidos no .env")
	}
	dsn := dbUser + ":" + dbPass + "@tcp(" + dbHost + ":" + dbPort + ")/" + dbName + "?charset=utf8mb4&parseTime=True&loc=Local"

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

	updates := bot.GetUpdatesChan(u)

	for update := range updates {
		if update.Message != nil && update.Message.Document != nil {
			fileID := update.Message.Document.FileID
			bot.Send(tgbotapi.NewMessage(update.Message.Chat.ID, "Arquivo recebido! A conversão está em andamento, por favor aguarde..."))
			file, err := bot.GetFile(tgbotapi.FileConfig{FileID: fileID})
			if err != nil {
				bot.Send(tgbotapi.NewMessage(update.Message.Chat.ID, "Erro ao baixar o arquivo."))
				continue
			}
			downloadURL := file.Link(bot.Token)
			inputFile := update.Message.Document.FileName
			if !strings.HasSuffix(inputFile, ".sql") {
				inputFile = "entrada.sql"
			}
			outputFile := strings.TrimSuffix(inputFile, ".sql") + ".json"
			err = downloadFile(downloadURL, inputFile)
			if err != nil {
				bot.Send(tgbotapi.NewMessage(update.Message.Chat.ID, "Erro ao salvar o arquivo."))
				continue
			}
			err = processarArquivoSQL(inputFile, outputFile)
			if err != nil {
				bot.Send(tgbotapi.NewMessage(update.Message.Chat.ID, "Erro ao processar o arquivo: "+err.Error()))
				continue
			}
			// Enviar para MySQL após gerar o JSON
			err = db.EnviarParaMySQL(outputFile, dsn)
			if err != nil {
				bot.Send(tgbotapi.NewMessage(update.Message.Chat.ID, "Erro ao enviar para o MySQL: "+err.Error()))
				continue
			}
			// Gerar backup do banco de dados via go-mysqldump
			backupDir := "backups"
			if err := os.MkdirAll(backupDir, 0755); err != nil {
				bot.Send(tgbotapi.NewMessage(update.Message.Chat.ID, "Erro ao criar diretório de backup: "+err.Error()))
				continue
			}
			inputFileBase := filepath.Base(inputFile)
			backupFile := filepath.Join(backupDir, strings.TrimSuffix(inputFileBase, ".sql")+"-convertido.sql")
			absBackupFile, err := filepath.Abs(backupFile)
			if err != nil {
				bot.Send(tgbotapi.NewMessage(update.Message.Chat.ID, "Erro ao obter caminho absoluto do backup: "+err.Error()))
				continue
			}
			dbConn, err := db.OpenDB(dsn)
			if err != nil {
				bot.Send(tgbotapi.NewMessage(update.Message.Chat.ID, "Erro ao conectar para backup: "+err.Error()))
				continue
			}
			dumper, err := mysqldump.Register(dbConn, absBackupFile, dbName)
			if err != nil {
				bot.Send(tgbotapi.NewMessage(update.Message.Chat.ID, "Erro ao preparar backup: "+err.Error()))
				dbConn.Close()
				continue
			}
			_, err = dumper.Dump()
			dumper.Close()
			dbConn.Close()
			if err != nil {
				bot.Send(tgbotapi.NewMessage(update.Message.Chat.ID, "Erro ao gerar backup do banco: "+err.Error()))
				continue
			}
			doc := tgbotapi.NewDocument(update.Message.Chat.ID, tgbotapi.FilePath(backupFile))
			bot.Send(doc)
		} else if update.Message != nil {
			bot.Send(tgbotapi.NewMessage(update.Message.Chat.ID, "Envie um arquivo .sql para converter em .json."))
		}
	}
}

func splitFields(row string) []string {
	var fields []string
	var field string
	inQuote := false
	quoteChar := byte(0)

	for i := 0; i < len(row); i++ {
		char := row[i]
		if (char == '\'' || char == '"') && !inQuote {
			inQuote = true
			quoteChar = char
			continue
		}
		if inQuote && char == quoteChar {
			inQuote = false
			continue
		}
		if char == ',' && !inQuote {
			fields = append(fields, field)
			field = ""
		} else {
			field += string(char)
		}
	}
	if field != "" {
		fields = append(fields, field)
	}
	return fields
}

func parseCategoria(fields []string) Categoria {
	var cat Categoria
	fmt.Sscanf(fields[0], "%d", &cat.ID)
	fmt.Sscanf(fields[1], "%d", &cat.SubID)
	cat.Nome = fields[2]
	return cat
}

func parseUsuario(fields []string) Usuario {
	for len(fields) < 17 {
		fields = append(fields, "")
	}
	var user Usuario
	fmt.Sscanf(fields[0], "%d", &user.ID)
	fmt.Sscanf(fields[1], "%d", &user.MainID)
	fmt.Sscanf(fields[2], "%d", &user.SubID)
	user.Login = fields[3]
	user.Senha = fields[4]
	user.Nome = fields[5]
	user.Validade = fields[6]
	fmt.Sscanf(fields[7], "%f", &user.Valor)
	fmt.Sscanf(fields[8], "%d", &user.Notificado)
	user.Whatsapp = fields[9]
	user.UUID = fields[10]
	fmt.Sscanf(fields[11], "%d", &user.Status)
	fmt.Sscanf(fields[12], "%d", &user.Limite)
	fmt.Sscanf(fields[13], "%d", &user.Suspenso)
	fmt.Sscanf(fields[14], "%d", &user.Periodo)
	fmt.Sscanf(fields[15], "%d", &user.Teste)
	user.Telegram = fields[16]
	return user
}

func parseRevenda(fields []string) Revenda {
	var rev Revenda
	fmt.Sscanf(fields[0], "%d", &rev.ID)
	fmt.Sscanf(fields[1], "%d", &rev.MainID)
	rev.Login = fields[2]
	rev.Senha = fields[3]
	rev.Numero = fields[4]
	fmt.Sscanf(fields[5], "%f", &rev.Valor)
	fmt.Sscanf(fields[6], "%d", &rev.Limite)
	rev.Modo = fields[7]
	rev.Data = fields[8]
	fmt.Sscanf(fields[9], "%d", &rev.LimiteUse)
	fmt.Sscanf(fields[10], "%d", &rev.Categoria)
	fmt.Sscanf(fields[11], "%d", &rev.Sub)
	fmt.Sscanf(fields[12], "%d", &rev.Expirado)
	rev.TextoRev = fields[13]
	rev.TextoUser = fields[14]
	rev.APIKey = fields[15]
	fmt.Sscanf(fields[16], "%d", &rev.Notificado)
	rev.TextoTeste = fields[17]
	fmt.Sscanf(fields[18], "%f", &rev.ValorTeste)
	fmt.Sscanf(fields[19], "%d", &rev.V2RayTeste)
	return rev
}

func getDonoRevenda(rev Revenda, db Database) string {
	if rev.MainID == 1 || rev.ID == 1 {
		return "admin"
	}
	for _, r := range db.Revendas {
		if r.ID == rev.MainID {
			return r.Login
		}
	}
	return "desconhecido"
}

func getDonoUsuario(user Usuario, db Database) string {
	if user.MainID == 1 || user.ID == 1 {
		return "admin"
	}
	for _, r := range db.Revendas {
		if r.ID == user.MainID {
			return r.Login
		}
	}
	return "desconhecido"
}

func getNomeCategoriaPorSubID(subid int, db Database) string {
	for _, c := range db.Categorias {
		if c.SubID == subid {
			return c.Nome
		}
	}
	return "categoria não encontrada"
}

func gerarContatoAleatorio() string {
	ddd := rand.Intn(90) + 10
	numero := rand.Intn(900000000) + 100000000
	return fmt.Sprintf("55%d9%d", ddd, numero)
}
