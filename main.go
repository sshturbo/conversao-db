package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"conversao-db/internal/conversao"
	"conversao-db/internal/db"

	_ "github.com/go-sql-driver/mysql"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/joho/godotenv"
)

func checkLock() bool {
	lockFile := "bot.lock"
	file, err := os.OpenFile(lockFile, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0666)
	if err != nil {
		if os.IsExist(err) {
			// Verifica se o processo anterior ainda está rodando
			content, err := os.ReadFile(lockFile)
			if err != nil {
				return false
			}
			pid := string(content)
			if pid != "" {
				// Verifica se o processo ainda existe
				processPid := 0
				fmt.Sscanf(pid, "%d", &processPid)
				if processPid > 0 {
					process, err := os.FindProcess(processPid)
					if err == nil {
						err = process.Signal(syscall.Signal(0))
						if err == nil {
							return false // Processo ainda está rodando
						}
					}
				}
			}
		}
		// Se chegou aqui, o arquivo de lock existe mas o processo não
		os.Remove(lockFile)
		file, err = os.OpenFile(lockFile, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0666)
		if err != nil {
			return false
		}
	}
	// Escreve o PID atual no arquivo de lock
	pid := fmt.Sprintf("%d", os.Getpid())
	file.WriteString(pid)
	file.Close()
	return true
}

func removeLock() {
	os.Remove("bot.lock")
}

func main() {
	// Verifica se já existe uma instância rodando
	if !checkLock() {
		log.Fatal("Outra instância do bot já está em execução")
	}
	defer removeLock()

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

			err = conversao.DownloadFile(downloadURL, inputFile)
			if err != nil {
				bot.Send(tgbotapi.NewMessage(update.Message.Chat.ID, "Erro ao salvar o arquivo."))
				continue
			}

			// Processar SQL e obter estrutura em memória
			dbExport, err := conversao.ProcessarArquivoSQL(inputFile)
			if err != nil {
				bot.Send(tgbotapi.NewMessage(update.Message.Chat.ID, "Erro ao processar o arquivo: "+err.Error()))
				continue
			}

			// Enviar dados direto para MySQL
			err = db.EnviarParaMySQL(dbExport, dsn)
			if err != nil {
				bot.Send(tgbotapi.NewMessage(update.Message.Chat.ID, "Erro ao enviar para o MySQL: "+err.Error()))
				continue
			}

			// Gerar backup do banco de dados usando mysqldump
			backupDir := "backups"
			if err := os.MkdirAll(backupDir, 0755); err != nil {
				bot.Send(tgbotapi.NewMessage(update.Message.Chat.ID, "Erro ao criar diretório de backup: "+err.Error()))
				continue
			}

			inputFileBase := filepath.Base(inputFile)
			backupFileName := strings.TrimSuffix(inputFileBase, ".sql") + "-convertido.sql"
			backupFile := filepath.Join(backupDir, backupFileName)

			// Comando mysqldump
			cmd := exec.Command("mysqldump",
				"--host="+dbHost,
				"--port="+dbPort,
				"--protocol=tcp",
				"--user="+dbUser,
				"--password="+dbPass,
				"--databases", dbName,
				"--result-file="+backupFile,
				"--single-transaction",
				"--set-gtid-purged=OFF")

			output, err := cmd.CombinedOutput()
			if err != nil {
				errMsg := fmt.Sprintf("Erro ao gerar backup: %v\nSaída: %s", err, string(output))
				bot.Send(tgbotapi.NewMessage(update.Message.Chat.ID, errMsg))
				continue
			}

			// Verifica se o arquivo foi criado
			if _, err := os.Stat(backupFile); err != nil {
				bot.Send(tgbotapi.NewMessage(update.Message.Chat.ID, "Erro: arquivo de backup não foi criado"))
				continue
			}

			// Informa que o backup foi gerado e será enviado
			bot.Send(tgbotapi.NewMessage(update.Message.Chat.ID, "Backup gerado com sucesso! Enviando arquivo..."))

			// Prepara o documento para envio
			doc := tgbotapi.NewDocument(update.Message.Chat.ID, tgbotapi.FilePath(backupFile))
			doc.Caption = "Backup do banco de dados"

			// Tenta enviar o arquivo
			_, err = bot.Send(doc)
			if err != nil {
				errMsg := fmt.Sprintf("Erro ao enviar o backup: %v", err)
				bot.Send(tgbotapi.NewMessage(update.Message.Chat.ID, errMsg))
				continue
			}

			// Confirma o envio
			bot.Send(tgbotapi.NewMessage(update.Message.Chat.ID, "Backup enviado com sucesso!"))

			// Remove o arquivo SQL original após processamento
			os.Remove(inputFile)
		} else if update.Message != nil {
			bot.Send(tgbotapi.NewMessage(update.Message.Chat.ID, "Envie um arquivo .sql para converter em .json."))
		}
	}
}
