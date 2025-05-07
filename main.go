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
	"conversao-db/internal/state"

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
	// Obtém a escolha do usuário
	dbChoice := state.GetUserDatabaseChoice(job.ChatID)

	// Notifica início do processamento
	bot.Send(tgbotapi.NewMessage(job.ChatID, fmt.Sprintf("Iniciando processamento do seu arquivo para o banco %s...", dbChoice)))

	inputFile := job.FileName
	if !strings.HasSuffix(inputFile, ".sql") {
		inputFile = "entrada.sql"
	}

	err := conversao.DownloadFile(job.DownloadURL, inputFile)
	if err != nil {
		bot.Send(tgbotapi.NewMessage(job.ChatID, "Erro ao salvar o arquivo."))
		return
	}

	var dbExport interface{}
	var errProcess error

	switch dbChoice {
	case state.Atlas:
		// Processar no formato Atlas
		dbExport, errProcess = conversao.ProcessarArquivoSQLFinal(inputFile)
		if errProcess != nil {
			bot.Send(tgbotapi.NewMessage(job.ChatID, "Erro ao processar o arquivo: "+errProcess.Error()))
			return
		}
		err = db.EnviarParaMySQLFinal(dbExport.(*conversao.DatabaseFinal), dsn)
	case state.Eclipse:
		// Processar no formato Eclipse (original)
		dbExport, errProcess = conversao.ProcessarArquivoSQL(inputFile)
		if errProcess != nil {
			bot.Send(tgbotapi.NewMessage(job.ChatID, "Erro ao processar o arquivo: "+errProcess.Error()))
			return
		}
		err = db.EnviarParaMySQL(dbExport.(*conversao.DatabaseExport), dsn)
	default:
		bot.Send(tgbotapi.NewMessage(job.ChatID, "Erro: tipo de banco de dados não selecionado."))
		return
	}

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
		"--no-create-db",
		dbName,
		"--result-file="+backupFile,
		"--single-transaction")

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

func init() {
	// Carrega o arquivo .env
	if err := godotenv.Load(); err != nil {
		log.Fatal("Erro ao carregar arquivo .env")
	}

	// Carrega as variáveis de ambiente
	dbHost = os.Getenv("DB_HOST")
	dbPort = os.Getenv("DB_PORT")
	dbUser = os.Getenv("DB_USER")
	dbPass = os.Getenv("DB_PASS")
	dbName = os.Getenv("DB_NAME")

	// Verifica se todas as variáveis necessárias estão definidas
	if dbHost == "" || dbPort == "" || dbUser == "" || dbPass == "" || dbName == "" {
		log.Fatal("Variáveis de ambiente necessárias não encontradas no arquivo .env")
	}
}

func main() {
	if !checkLock() {
		log.Fatal("Uma instância do bot já está em execução")
	}
	defer removeLock()

	// Carregar token do bot do arquivo .env
	token := os.Getenv("BOT_TOKEN")
	if token == "" {
		log.Fatal("Token do bot não encontrado no arquivo .env")
	}

	// Monta a string de conexão usando as variáveis de ambiente
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbUser, dbPass, dbHost, dbPort, dbName)

	// Inicializa o bot
	bot, err := tgbotapi.NewBotAPI(token)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Bot autorizado na conta %s", bot.Self.UserName)

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

	updates := bot.GetUpdatesChan(u)

	// Cria a fila de trabalho
	workQueue := NewWorkQueue(1)
	workQueue.ProcessJobs(bot, dsn)

	// Canal para processar um arquivo por vez
	for update := range updates {
		if update.Message == nil && update.CallbackQuery == nil {
			continue
		}

		// Tratamento de callbacks (botões)
		if update.CallbackQuery != nil {
			callback := update.CallbackQuery
			chatID := callback.Message.Chat.ID

			switch callback.Data {
			case "eclipse":
				state.SetUserDatabaseChoice(chatID, state.Eclipse)
				msg := "Você escolheu o banco Eclipse. Por favor, envie o arquivo SQL para conversão."
				bot.Send(tgbotapi.NewMessage(chatID, msg))
			case "atlas":
				state.SetUserDatabaseChoice(chatID, state.Atlas)
				msg := "Você escolheu o banco Atlas. Por favor, envie o arquivo SQL para conversão."
				bot.Send(tgbotapi.NewMessage(chatID, msg))
			}

			// Responde ao callback
			bot.Send(tgbotapi.NewCallback(callback.ID, ""))
			continue
		}

		// Tratamento de mensagens
		msg := update.Message
		if msg == nil {
			continue
		}

		// Comando /start
		if msg.Command() == "start" {
			// Limpa estado anterior do usuário
			state.ClearUserState(msg.Chat.ID)

			// Cria teclado inline com os botões
			keyboard := tgbotapi.NewInlineKeyboardMarkup(
				tgbotapi.NewInlineKeyboardRow(
					tgbotapi.NewInlineKeyboardButtonData("Eclipse", "eclipse"),
					tgbotapi.NewInlineKeyboardButtonData("Atlas", "atlas"),
				),
			)

			reply := tgbotapi.NewMessage(msg.Chat.ID,
				"Bem-vindo ao Conversor de Banco de Dados!\n\n"+
					"Por favor, escolha o formato do banco de dados:")
			reply.ReplyMarkup = keyboard
			bot.Send(reply)
			continue
		}

		// Verifica se é um arquivo
		if msg.Document != nil {
			// Verifica se o usuário já escolheu o tipo de banco
			if state.GetUserDatabaseChoice(msg.Chat.ID) == "" {
				bot.Send(tgbotapi.NewMessage(msg.Chat.ID,
					"Por favor, use o comando /start primeiro para escolher o tipo de banco de dados."))
				continue
			}

			// Obtém informações do arquivo
			fileID := msg.Document.FileID
			fileName := msg.Document.FileName

			// Obtém URL do arquivo
			file, err := bot.GetFile(tgbotapi.FileConfig{FileID: fileID})
			if err != nil {
				bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "Erro ao obter o arquivo."))
				continue
			}

			// Adiciona trabalho à fila
			workQueue.AddJob(ConversionJob{
				ChatID:      msg.Chat.ID,
				FileID:      fileID,
				FileName:    fileName,
				DownloadURL: file.Link(token),
			})
		}
	}
}
