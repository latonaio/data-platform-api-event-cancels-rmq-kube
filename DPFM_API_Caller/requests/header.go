package requests

type Header struct {
	Event			int     `json:"Event"`
	IsCancelled		*bool	`json:"IsCancelled"`
}
