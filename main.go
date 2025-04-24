package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"conversao-db/internal/conversao"
	"conversao-db/internal/db"

	_ "github.com/go-sql-driver/mysql"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/joho/godotenv"
)

// ConversionJob representa um trabalho de conversão na fila
type ConversionJob struct {
	ChatID      int64
	FileID      string
	FileName    string
	DownloadURL string
}

// WorkQueue gerencia a fila de trabalhos
type WorkQueue struct {
	jobs    chan ConversionJob
	working sync.WaitGroup
}

// NewWorkQueue cria uma nova fila de trabalhos
func NewWorkQueue(numWorkers int) *WorkQueue {
	return &WorkQueue{
		jobs: make(chan ConversionJob, 100), // buffer para até 100 trabalhos
	}
}

// AddJob adiciona um novo trabalho à fila
func (wq *WorkQueue) AddJob(job ConversionJob) {
	wq.jobs <- job
}

// ProcessJobs inicia o processamento de trabalhos
func (wq *WorkQueue) ProcessJobs(bot *tgbotapi.BotAPI, dsn string) {
	wq.working.Add(1)
	go func() {
		defer wq.working.Done()
		for job := range wq.jobs {
			// Notifica o usuário que seu arquivo está na fila
			bot.Send(tgbotapi.NewMessage(job.ChatID, "Seu arquivo está na fila de processamento. Aguarde..."))

			// Processa o trabalho
			processConversionJob(bot, job, dsn)

			// Pequena pausa entre processamentos
			time.Sleep(1 * time.Second)
		}
	}()
}

// processConversionJob processa um trabalho de conversão individual
func processConversionJob(bot *tgbotapi.BotAPI, job ConversionJob, dsn string) {
	// Notifica início do processamento
	bot.Send(tgbotapi.NewMessage(job.ChatID, "Iniciando processamento do seu arquivo..."))

	inputFile := job.FileName
	if !strings.HasSuffix(inputFile, ".sql") {
		inputFile = "entrada.sql"
	}

	err := conversao.DownloadFile(job.DownloadURL, inputFile)
	if err != nil {
		bot.Send(tgbotapi.NewMessage(job.ChatID, "Erro ao salvar o arquivo."))
		return
	}

	// Processar SQL e obter estrutura em memória
	dbExport, err := conversao.ProcessarArquivoSQL(inputFile)
	if err != nil {
		bot.Send(tgbotapi.NewMessage(job.ChatID, "Erro ao processar o arquivo: "+err.Error()))
		return
	}

	// Enviar dados direto para MySQL
	err = db.EnviarParaMySQL(dbExport, dsn)
	if err != nil {
		bot.Send(tgbotapi.NewMessage(job.ChatID, "Erro ao enviar para o MySQL: "+err.Error()))
		return
	}

	// Gerar backup do banco de dados usando mysqldump
	backupDir := "backups"
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		bot.Send(tgbotapi.NewMessage(job.ChatID, "Erro ao criar diretório de backup: "+err.Error()))
		return
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
		"--default-character-set=utf8mb4",
		"--set-charset=utf8mb4",
		"--skip-set-charset",
		"--no-create-db",
		dbName,
		"--result-file="+backupFile,
		"--single-transaction",
		"--set-gtid-purged=OFF")

	output, err := cmd.CombinedOutput()
	if err != nil {
		errMsg := fmt.Sprintf("Erro ao gerar backup: %v\nSaída: %s", err, string(output))
		bot.Send(tgbotapi.NewMessage(job.ChatID, errMsg))
		return
	}

	// Verifica se o arquivo foi criado
	if _, err := os.Stat(backupFile); err != nil {
		bot.Send(tgbotapi.NewMessage(job.ChatID, "Erro: arquivo de backup não foi criado"))
		return
	}

	// Informa que o backup foi gerado e será enviado
	bot.Send(tgbotapi.NewMessage(job.ChatID, "Backup gerado com sucesso! Enviando arquivo..."))

	// Prepara o documento para envio
	doc := tgbotapi.NewDocument(job.ChatID, tgbotapi.FilePath(backupFile))
	doc.Caption = "Backup do banco de dados"

	// Tenta enviar o arquivo
	_, err = bot.Send(doc)
	if err != nil {
		errMsg := fmt.Sprintf("Erro ao enviar o backup: %v", err)
		bot.Send(tgbotapi.NewMessage(job.ChatID, errMsg))
		return
	}

	// Confirma o envio
	bot.Send(tgbotapi.NewMessage(job.ChatID, "Backup enviado com sucesso!"))

	// Remove o arquivo de backup local após envio bem sucedido
	err = os.Remove(backupFile)
	if err != nil {
		errMsg := fmt.Sprintf("Aviso: não foi possível remover o arquivo de backup local: %v", err)
		bot.Send(tgbotapi.NewMessage(job.ChatID, errMsg))
	}

	// Limpar as tabelas para deixar o banco pronto para a próxima importação
	dbConn, err := db.OpenDB(dsn)
	if err != nil {
		errMsg := fmt.Sprintf("Erro ao conectar ao banco para limpeza: %v", err)
		bot.Send(tgbotapi.NewMessage(job.ChatID, errMsg))
	} else {
		err = db.LimparTabelas(dbConn)
		if err != nil {
			errMsg := fmt.Sprintf("Erro ao limpar tabelas: %v", err)
			bot.Send(tgbotapi.NewMessage(job.ChatID, errMsg))
		} else {
			bot.Send(tgbotapi.NewMessage(job.ChatID, "Tabelas limpas com sucesso! Banco pronto para próxima importação."))
		}
		dbConn.Close()
	}

	// Remove o arquivo SQL original após processamento
	os.Remove(inputFile)
}

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

var (
	dbHost string
	dbPort string
	dbUser string
	dbPass string
	dbName string
)

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

	dbUser = os.Getenv("DB_USER")
	dbPass = os.Getenv("DB_PASS")
	dbHost = os.Getenv("DB_HOST")
	dbPort = os.Getenv("DB_PORT")
	dbName = os.Getenv("DB_NAME")
	if dbUser == "" || dbPass == "" || dbHost == "" || dbPort == "" || dbName == "" {
		log.Fatal("Dados de conexão do banco não definidos no .env")
	}
	dsn := dbUser + ":" + dbPass + "@tcp(" + dbHost + ":" + dbPort + ")/" + dbName + "?charset=utf8mb4&collation=utf8mb4_unicode_ci&parseTime=True&loc=Local"

	// Inicializar a fila de trabalhos
	workQueue := NewWorkQueue(1) // 1 worker para processar um arquivo por vez
	workQueue.ProcessJobs(bot, dsn)

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

	updates := bot.GetUpdatesChan(u)

	for update := range updates {
		if update.Message != nil && update.Message.Document != nil {
			fileID := update.Message.Document.FileID
			bot.Send(tgbotapi.NewMessage(update.Message.Chat.ID, "Arquivo recebido! Adicionando à fila de conversão..."))

			file, err := bot.GetFile(tgbotapi.FileConfig{FileID: fileID})
			if err != nil {
				bot.Send(tgbotapi.NewMessage(update.Message.Chat.ID, "Erro ao baixar o arquivo."))
				continue
			}

			// Criar um novo trabalho e adicionar à fila
			job := ConversionJob{
				ChatID:      update.Message.Chat.ID,
				FileID:      fileID,
				FileName:    update.Message.Document.FileName,
				DownloadURL: file.Link(bot.Token),
			}
			workQueue.AddJob(job)

		} else if update.Message != nil {
			bot.Send(tgbotapi.NewMessage(update.Message.Chat.ID, "Envie um arquivo .sql para conversão."))
		}
	}
}
