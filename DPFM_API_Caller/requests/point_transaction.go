package requests

type PointTransaction struct {
	Event							int     `json:"Event"`
	Sender							int		`json:"Sender"`
	Receiver						int		`json:"Receiver"`
	PointConditionRecord			int		`json:"PointConditionRecord"`
	PointConditionSequentialNumber	int		`json:"PointConditionSequentialNumber"`
	IsCancelled						*bool	`json:"IsCancelled"`
}
