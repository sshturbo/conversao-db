package state

import "sync"

type DatabaseType string

const (
	Eclipse DatabaseType = "eclipse"
	Atlas   DatabaseType = "atlas"
)

type UserState struct {
	DatabaseChoice DatabaseType
}

var (
	userStates = make(map[int64]*UserState)
	stateMutex sync.RWMutex
)

// SetUserDatabaseChoice define a escolha do banco de dados para um usuário
func SetUserDatabaseChoice(chatID int64, dbType DatabaseType) {
	stateMutex.Lock()
	defer stateMutex.Unlock()

	if _, exists := userStates[chatID]; !exists {
		userStates[chatID] = &UserState{}
	}
	userStates[chatID].DatabaseChoice = dbType
}

// GetUserDatabaseChoice retorna a escolha do banco de dados do usuário
func GetUserDatabaseChoice(chatID int64) DatabaseType {
	stateMutex.RLock()
	defer stateMutex.RUnlock()

	if state, exists := userStates[chatID]; exists {
		return state.DatabaseChoice
	}
	return ""
}

// ClearUserState limpa o estado do usuário
func ClearUserState(chatID int64) {
	stateMutex.Lock()
	defer stateMutex.Unlock()
	delete(userStates, chatID)
}
