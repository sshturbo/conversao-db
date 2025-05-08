package conversao

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// DatabaseFinal representa a estrutura dos dados no formato final
type DatabaseFinal struct {
	Accounts    []AccountFinal
	SSHAccounts []SSHAccountFinal
	Atribuidos  []AtribuidoFinal
	Categorias  []CategoriaFinal
}

type AccountFinal struct {
	ID            int    `json:"id"`
	Nome          string `json:"nome"`
	Contato       string `json:"contato"`
	Email         string `json:"email"`
	Login         string `json:"login"`
	Token         string `json:"token"`
	MB            string `json:"mb"`
	Senha         string `json:"senha"`
	ByID          string `json:"byid"`
	MainID        string `json:"mainid"`
	AccessToken   string `json:"accesstoken"`
	ValorUsuario  string `json:"valorusuario"`
	ValorRevenda  string `json:"valorrevenda"`
	IDTelegram    string `json:"idtelegram"`
	Tempo         string `json:"tempo"`
	TokenVenda    string `json:"tokenvenda"`
	TokenPagHiper string `json:"tokenpaghiper"`
	FormaDePag    string `json:"formadepag"`
	WhatsApp      string `json:"whatsapp"`
	Nivel         int    `json:"nivel"`
}

type SSHAccountFinal struct {
	ID          int    `json:"id"`
	ByID        int    `json:"byid"`
	CategoriaID int    `json:"categoriaid"`
	Limite      int    `json:"limite"`
	ByCredit    int    `json:"bycredit"`
	Login       string `json:"login"`
	Nome        string `json:"nome"`
	Senha       string `json:"senha"`
	MainID      string `json:"mainid"`
	Expira      string `json:"expira"`
	LastView    string `json:"lastview"`
	Status      string `json:"status"`
	ValorMensal string `json:"valormensal"`
	Notificado  string `json:"notificado"`
	WhatsApp    string `json:"whatsapp"`
	UUID        string `json:"uuid"`
	DeviceID    string `json:"deviceid"`
	DeviceAtivo string `json:"deviceativo"`
	Contato     string `json:"contato"`
	Tipo        string `json:"tipo"`
}

type AtribuidoFinal struct {
	ID          int    `json:"id"`
	Valor       string `json:"valor"`
	CategoriaID int    `json:"categoriaid"`
	UserID      int    `json:"userid"`
	ByID        int    `json:"byid"`
	Limite      int    `json:"limite"`
	LimiteTest  int    `json:"limitetest"`
	Tipo        string `json:"tipo"`
	Expira      string `json:"expira"`
	SubRev      int    `json:"subrev"`
	Suspenso    *int   `json:"suspenso"`
	ValorMensal string `json:"valormensal"`
	Notificado  string `json:"notificado"`
	SusID       *int   `json:"susid"`
}

type CategoriaFinal struct {
	ID    int    `json:"id"`
	SubID int    `json:"subid"`
	Nome  string `json:"nome"`
}

// ProcessarArquivoSQLFinal processa um arquivo SQL que já está no formato final
func ProcessarArquivoSQLFinal(inputFile string) (*DatabaseFinal, error) {
	file, err := os.Open(inputFile)
	if err != nil {
		return nil, fmt.Errorf("erro ao abrir arquivo: %v", err)
	}
	defer file.Close()

	db := &DatabaseFinal{
		Accounts:    make([]AccountFinal, 0),
		SSHAccounts: make([]SSHAccountFinal, 0),
		Atribuidos:  make([]AtribuidoFinal, 0),
		Categorias:  make([]CategoriaFinal, 0),
	}

	scanner := bufio.NewScanner(file)
	inInsert := false
	currentTable := ""
	var values string

	for scanner.Scan() {
		line := scanner.Text()

		if strings.HasPrefix(line, "DROP TABLE") || strings.HasPrefix(line, "CREATE TABLE") {
			continue
		}

		// Accounts
		if strings.HasPrefix(line, "INSERT INTO `accounts`") || strings.HasPrefix(line, "INSERT INTO accounts") {
			inInsert = true
			currentTable = "accounts"
			values = ""
			if idx := strings.Index(line, "VALUES"); idx != -1 {
				values = line[idx+6:] // Pega só o trecho após VALUES
				if strings.HasSuffix(values, ";") {
					values = strings.TrimSuffix(values, ";")
					processValues(values, currentTable, db)
					inInsert = false
				}
			}
		} else if strings.HasPrefix(line, "INSERT INTO `ssh_accounts`") || strings.HasPrefix(line, "INSERT INTO ssh_accounts") {
			inInsert = true
			currentTable = "ssh_accounts"
			values = ""
			if idx := strings.Index(line, "VALUES"); idx != -1 {
				values = line[idx+6:]
				if strings.HasSuffix(values, ";") {
					values = strings.TrimSuffix(values, ";")
					processValues(values, currentTable, db)
					inInsert = false
				}
			}
		} else if strings.HasPrefix(line, "INSERT INTO `atribuidos`") || strings.HasPrefix(line, "INSERT INTO atribuidos") {
			inInsert = true
			currentTable = "atribuidos"
			values = ""
			if idx := strings.Index(line, "VALUES"); idx != -1 {
				values = line[idx+6:]
				if strings.HasSuffix(values, ";") {
					values = strings.TrimSuffix(values, ";")
					processValues(values, currentTable, db)
					inInsert = false
				}
			}
		} else if strings.HasPrefix(line, "INSERT INTO `categorias`") || strings.HasPrefix(line, "INSERT INTO categorias") {
			inInsert = true
			currentTable = "categorias"
			values = ""
			if idx := strings.Index(line, "VALUES"); idx != -1 {
				values = line[idx+6:]
				if strings.HasSuffix(values, ";") {
					values = strings.TrimSuffix(values, ";")
					processValues(values, currentTable, db)
					inInsert = false
				}
			}
		}

		if inInsert {
			values += line
			if strings.HasSuffix(line, ";") {
				values = strings.TrimSuffix(values, ";")
				processValues(values, currentTable, db)
				inInsert = false
			}
		}
	}

	// --- AJUSTE MAINID DOS ACCOUNTS (exceto admin) ---
	mainidMap := make(map[int]string)
	for i, acc := range db.Accounts {
		if acc.ID == 1 {
			db.Accounts[i].MainID = "0"
			mainidMap[acc.ID] = "0"
		} else {
			if acc.MainID == "" || acc.MainID == "0" {
				mainid := fmt.Sprintf("%d", GerarMainID())
				db.Accounts[i].MainID = mainid
				mainidMap[acc.ID] = mainid
			} else {
				mainidMap[acc.ID] = acc.MainID
			}
		}
	}
	// --- AJUSTE MAINID DOS SSH_ACCOUNTS ---
	for i, ssh := range db.SSHAccounts {
		mainid := mainidMap[ssh.ByID]
		if mainid == "" {
			mainid = "0"
		}
		db.SSHAccounts[i].MainID = mainid
	}

	return db, nil
}

func processValues(values string, table string, db *DatabaseFinal) {
	values = strings.TrimPrefix(values, "VALUES")
	values = strings.TrimSpace(values)

	// Garante que começa exatamente no primeiro '('
	idx := strings.Index(values, "(")
	if idx != -1 {
		values = values[idx:]
	}

	// Remove possível vírgula final
	values = strings.TrimSuffix(values, ";")

	// Divide os registros corretamente
	rows := splitInsertRows(values)

	for _, row := range rows {
		row = strings.Trim(row, "() ")
		fields := splitFieldsFinal(row)
		if len(fields) == 0 {
			continue
		}
		if isHeaderRow(fields) {
			continue
		}

		switch table {
		case "accounts":
			acc := parseAccountFinal(fields)
			db.Accounts = append(db.Accounts, acc)
		case "ssh_accounts":
			ssh := parseSSHAccountFinal(fields)
			db.SSHAccounts = append(db.SSHAccounts, ssh)
		case "atribuidos":
			atr := parseAtribuidoFinal(fields)
			db.Atribuidos = append(db.Atribuidos, atr)
		case "categorias":
			cat := parseCategoriaFinal(fields)
			db.Categorias = append(db.Categorias, cat)
		}
	}
}

// Função robusta para dividir os registros do insert
func splitInsertRows(values string) []string {
	var rows []string
	var buf strings.Builder
	open := 0
	for i := 0; i < len(values); i++ {
		c := values[i]
		if c == '(' {
			if open == 0 && buf.Len() > 0 {
				buf.Reset()
			}
			open++
		}
		if open > 0 {
			buf.WriteByte(c)
		}
		if c == ')' {
			open--
			if open == 0 {
				rows = append(rows, buf.String())
				buf.Reset()
			}
		}
	}
	return rows
}

// Função auxiliar para detectar se a linha é header de nomes de colunas
func isHeaderRow(fields []string) bool {
	colunasPossiveis := map[string]bool{
		"id": true, "nome": true, "contato": true, "email": true, "login": true, "token": true, "mb": true, "senha": true,
		"byid": true, "mainid": true, "accesstoken": true, "valorusuario": true, "valorrevenda": true,
		"idtelegram": true, "tempo": true, "tokenvenda": true, "acesstokenpaghiper": true, "formadepag": true,
		"tokenpaghiper": true, "whatsapp": true, "categoriaid": true, "userid": true, "limite": true,
		"limitetest": true, "tipo": true, "expira": true, "subrev": true, "suspenso": true, "valormensal": true,
		"notificado": true, "susid": true, "subid": true,
	}
	count := 0
	for _, f := range fields {
		nome := strings.ToLower(strings.Trim(f, " `'\""))
		// Se o campo for só letras e for uma coluna possível, conta
		if colunasPossiveis[nome] {
			count++
		}
		// Se o campo for só letras/crases/aspas, provavelmente é header
		if len(nome) > 0 && isOnlyLetters(nome) && colunasPossiveis[nome] {
			count++
		}
	}
	// Se a maioria dos campos são nomes de colunas, é header
	return count > len(fields)/2 && len(fields) > 0
}

// Função auxiliar para verificar se a string contém apenas letras
func isOnlyLetters(s string) bool {
	for _, r := range s {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') {
			return false
		}
	}
	return true
}

// Função auxiliar para dividir os campos de uma linha SQL considerando aspas
func splitFieldsFinal(row string) []string {
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

func parseAccountFinal(fields []string) AccountFinal {
	var acc AccountFinal
	fmt.Sscanf(fields[0], "%d", &acc.ID)
	acc.Nome = strings.TrimSpace(fields[1])
	acc.Contato = strings.TrimSpace(fields[2])
	acc.Login = strings.TrimSpace(fields[3])
	acc.Token = strings.TrimSpace(fields[4])
	acc.MB = strings.TrimSpace(fields[5])
	acc.Senha = strings.TrimSpace(fields[6])
	acc.ByID = strings.TrimSpace(fields[7])
	acc.MainID = strings.TrimSpace(fields[8])
	acc.AccessToken = strings.TrimSpace(fields[9])
	acc.ValorUsuario = strings.TrimSpace(fields[10])
	acc.ValorRevenda = strings.TrimSpace(fields[11])
	acc.IDTelegram = strings.TrimSpace(fields[12])
	acc.Tempo = strings.TrimSpace(fields[13])
	acc.TokenVenda = strings.TrimSpace(fields[14])
	acc.TokenPagHiper = strings.TrimSpace(fields[15])
	acc.FormaDePag = strings.TrimSpace(fields[16])
	acc.WhatsApp = strings.TrimSpace(fields[17])

	// Nome: se vazio ou 'NULL', usar login
	if acc.Nome == "" || strings.ToUpper(acc.Nome) == "NULL" {
		acc.Nome = acc.Login
	}
	// Contato: se vazio ou 'NULL', usar número exemplo
	if acc.Contato == "" || strings.ToUpper(acc.Contato) == "NULL" {
		acc.Contato = "62999999999"
	}
	// Email: <login>@gmail.com
	acc.Email = acc.Login + "@gmail.com"
	// MainID: aleatório, exceto se id==1
	if acc.ID == 1 {
		acc.MainID = "0"
	} else {
		acc.MainID = fmt.Sprintf("%d", GerarMainID())
	}
	// Nivel: 3 se id==1, senão 2
	if acc.ID == 1 {
		acc.Nivel = 3
	} else {
		acc.Nivel = 2
	}
	return acc
}

func parseSSHAccountFinal(fields []string) SSHAccountFinal {
	// Garante que temos campos suficientes
	for len(fields) < 17 {
		fields = append(fields, "")
	}

	var ssh SSHAccountFinal

	// Campos obrigatórios com valores padrão se vazios
	if len(fields) > 0 {
		fmt.Sscanf(fields[0], "%d", &ssh.ID)
	}
	if len(fields) > 1 {
		fmt.Sscanf(fields[1], "%d", &ssh.ByID)
	}
	if len(fields) > 2 {
		fmt.Sscanf(fields[2], "%d", &ssh.CategoriaID)
	}
	if len(fields) > 3 {
		fmt.Sscanf(fields[3], "%d", &ssh.Limite)
	}
	if len(fields) > 4 {
		fmt.Sscanf(fields[4], "%d", &ssh.ByCredit)
	}

	// Campos string podem ser vazios
	if len(fields) > 5 {
		ssh.Login = strings.TrimSpace(fields[5])
	}
	// Nome: sempre igual ao login
	ssh.Nome = ssh.Login
	if len(fields) > 6 {
		ssh.Senha = strings.TrimSpace(fields[6])
	}
	if len(fields) > 7 {
		ssh.MainID = strings.TrimSpace(fields[7])
	}
	if len(fields) > 8 {
		ssh.Expira = strings.TrimSpace(fields[8])
	}
	if len(fields) > 9 {
		ssh.LastView = strings.TrimSpace(fields[9])
	}
	if len(fields) > 10 {
		ssh.Status = strings.TrimSpace(fields[10])
	}
	if len(fields) > 11 {
		ssh.ValorMensal = strings.TrimSpace(fields[11])
	}
	if len(fields) > 12 {
		ssh.Notificado = strings.TrimSpace(fields[12])
	}
	if len(fields) > 13 {
		ssh.WhatsApp = strings.TrimSpace(fields[13])
	}
	if len(fields) > 14 {
		ssh.UUID = strings.TrimSpace(fields[14])
	}
	if len(fields) > 15 {
		ssh.DeviceID = strings.TrimSpace(fields[15])
	}
	if len(fields) > 16 {
		ssh.DeviceAtivo = strings.TrimSpace(fields[16])
	}

	// Contato: número de exemplo
	ssh.Contato = "62999999999"
	// Tipo: sempre 'xray'
	ssh.Tipo = "xray"
	// Status: se vazio ou zero, definir como '1'
	if ssh.Status == "" || ssh.Status == "0" {
		ssh.Status = "1"
	}
	if ssh.MainID == "" {
		ssh.MainID = "0"
	}

	return ssh
}

func parseAtribuidoFinal(fields []string) AtribuidoFinal {
	var atr AtribuidoFinal
	fmt.Sscanf(fields[0], "%d", &atr.ID)
	atr.Valor = strings.TrimSpace(fields[1])
	fmt.Sscanf(fields[2], "%d", &atr.CategoriaID)
	fmt.Sscanf(fields[3], "%d", &atr.UserID)
	fmt.Sscanf(fields[4], "%d", &atr.ByID)
	fmt.Sscanf(fields[5], "%d", &atr.Limite)
	if len(fields) > 6 {
		fmt.Sscanf(fields[6], "%d", &atr.LimiteTest)
	}
	if len(fields) > 7 {
		atr.Tipo = strings.TrimSpace(fields[7])
	}
	if len(fields) > 8 {
		atr.Expira = strings.TrimSpace(fields[8])
	}
	if len(fields) > 9 {
		fmt.Sscanf(fields[9], "%d", &atr.SubRev)
	}
	if len(fields) > 10 {
		var suspenso int
		fmt.Sscanf(fields[10], "%d", &suspenso)
		if suspenso == 0 {
			atr.Suspenso = nil
		} else {
			atr.Suspenso = &suspenso
		}
	} else {
		atr.Suspenso = nil
	}
	if len(fields) > 11 {
		atr.ValorMensal = strings.TrimSpace(fields[11])
	}
	if len(fields) > 12 {
		atr.Notificado = strings.TrimSpace(fields[12])
	}
	// SusID sempre nulo
	atr.SusID = nil
	return atr
}

func parseCategoriaFinal(fields []string) CategoriaFinal {
	var cat CategoriaFinal
	fmt.Sscanf(fields[0], "%d", &cat.ID)
	fmt.Sscanf(fields[1], "%d", &cat.SubID)
	cat.Nome = strings.TrimSpace(fields[2])
	return cat
}
