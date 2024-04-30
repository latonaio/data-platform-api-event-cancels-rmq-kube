package requests

type Game struct {
	Event			int     `json:"Event"`
	Game			int     `json:"Game"`
	IsCancelled		*bool	`json:"IsCancelled"`
}
