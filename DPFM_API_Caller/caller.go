package dpfm_api_caller

import (
	"context"
	dpfm_api_input_reader "data-platform-api-event-cancels-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-event-cancels-rmq-kube/DPFM_API_Output_Formatter"
	"data-platform-api-event-cancels-rmq-kube/config"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
	database "github.com/latonaio/golang-mysql-network-connector"
	rabbitmq "github.com/latonaio/rabbitmq-golang-client-for-data-platform"
	"golang.org/x/xerrors"
)

type DPFMAPICaller struct {
	ctx  context.Context
	conf *config.Conf
	rmq  *rabbitmq.RabbitmqClient
	db   *database.Mysql
}

func NewDPFMAPICaller(
	conf *config.Conf, rmq *rabbitmq.RabbitmqClient, db *database.Mysql,
) *DPFMAPICaller {
	return &DPFMAPICaller{
		ctx:  context.Background(),
		conf: conf,
		rmq:  rmq,
		db:   db,
	}
}

func (c *DPFMAPICaller) AsyncCancels(
	accepter []string,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (interface{}, []error) {
	var response interface{}
	switch input.APIType {
	case "cancels":
		response = c.cancelSqlProcess(input, output, accepter, log)
	default:
		log.Error("unknown api type %s", input.APIType)
	}
	return response, nil
}

func (c *DPFMAPICaller) cancelSqlProcess(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	accepter []string,
	log *logger.Logger,
) *dpfm_api_output_formatter.Message {
	var headerData *dpfm_api_output_formatter.Header
	campaignData := make([]dpfm_api_output_formatter.Campaign, 0)
	gameData := make([]dpfm_api_output_formatter.Game, 0)
	pointTransactionData := make([]dpfm_api_output_formatter.PointTransaction, 0)
	for _, a := range accepter {
		switch a {
		case "Header":
			h, i, j, k := c.headerCancel(input, output, log)
			headerData = h
			if h == nil || i == nil || j == nil {
				continue
			}
			campaignData = append(campaignData, *i...)
			gameData = append(gameData, *j...)
			pointTransactionData = append(pointTransactionData, *k...)
		case "Campaign":
			i := c.campaignCancel(input, output, log)
			if i == nil {
				continue
			}
			campaignData = append(campaignData, *i...)
		case "Game":
			j := c.gameCancel(input, output, log)
			if j == nil {
				continue
			}
			gameData = append(gameData, *j...)
		case "PointTransaction":
			k := c.pointTransactionCancel(input, output, log)
			if k == nil {
				continue
			}
			pointTransactionData = append(pointTransactionData, *k...)
		}
	}

	return &dpfm_api_output_formatter.Message{
		Header:				headerData,
		Campaign:			&campaignData,
		Game:				&gameData,
		PointTransaction:	&pointTransactionData,
	}
}

func (c *DPFMAPICaller) headerCancel(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (*dpfm_api_output_formatter.Header, *[]dpfm_api_output_formatter.Campaign, *[]dpfm_api_output_formatter.Game, *[]dpfm_api_output_formatter.PointTransaction) {
	sessionID := input.RuntimeSessionID

	header := c.HeaderRead(input, log)
	if header == nil {
		return nil, nil, nil, nil
	}
	header.IsCancelled = input.Header.IsCancelled
	res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": header, "function": "EventHeader", "runtime_session_id": sessionID})
	if err != nil {
		err = xerrors.Errorf("rmq error: %w", err)
		log.Error("%+v", err)
		return nil, nil, nil, nil
	}
	res.Success()
	if !checkResult(res) {
		output.SQLUpdateResult = getBoolPtr(false)
		output.SQLUpdateError = "Header Data cannot cancel"
		return nil, nil, nil, nil
	}
	// headerのキャンセルが取り消された時は子に影響を与えない
	if !*header.IsCancelled {
		return header, nil, nil, nil
	}

	campaigns := c.CampaingsRead(input, log)
	for i := range *campaigns {
		(*campaigns)[i].IsCancelled = input.Header.IsCancelled
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": (*campaigns)[i], "function": "EventCampaign", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil, nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Event Campaign Data cannot cancel"
			return nil, nil, nil, nil
		}
	}

	games := c.GamesRead(input, log)
	for i := range *games {
		(*games)[i].IsCancelled = input.Header.IsCancelled
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": (*games)[i], "function": "EventGame", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil, nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Event Game Data cannot cancel"
			return nil, nil, nil, nil
		}
	}
	
	pointTransactions := c.PointTransactionsRead(input, log)
	for i := range *pointTransactions {
		(*pointTransactions)[i].IsCancelled = input.Header.IsCancelled
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": (*pointTransactions)[i], "function": "EventPointTransaction", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil, nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Event PointTransaction Data cannot cancel"
			return nil, nil, nil, nil
		}
	}
	
	return header, campaigns, games, pointTransactions
}

func (c *DPFMAPICaller) campaignCancel(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (*[]dpfm_api_output_formatter.Campaign) {
	sessionID := input.RuntimeSessionID
	campaign := input.Header.Campaign[0]

	campaigns := make([]dpfm_api_output_formatter.Campaign, 0)
	for _, v := range input.Header.Campaign {
		data := dpfm_api_output_formatter.Campaign{
			Event:				input.Header.Event,
			Campaign:			v.Campaign,
			IsCancelled:        v.IsCancelled,
		}
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": data, "function": "EventCampaign", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Event Campaign Data cannot cancel"
			return nil
		}
	}

	// campaignがキャンセル取り消しされた場合、headerのキャンセルも取り消す
	if !*input.Header.Campaign[0].IsCancelled {
		header := c.HeaderRead(input, log)
		header.IsCancelled = input.Header.Campaign[0].IsCancelled
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": header, "function": "EventHeader", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Header Data cannot cancel"
			return nil
		}
	}

	return &campaigns
}

func (c *DPFMAPICaller) gameCancel(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (*[]dpfm_api_output_formatter.Game) {
	sessionID := input.RuntimeSessionID
	game := input.Header.Game[0]

	games := make([]dpfm_api_output_formatter.Game, 0)
	for _, v := range input.Header.Game {
		data := dpfm_api_output_formatter.Game{
			Event:				input.Header.Event,
			Game:			v.Game,
			IsCancelled:        v.IsCancelled,
		}
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": data, "function": "EventGame", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Event Game Data cannot cancel"
			return nil
		}
	}

	// gameがキャンセル取り消しされた場合、headerのキャンセルも取り消す
	if !*input.Header.Game[0].IsCancelled {
		header := c.HeaderRead(input, log)
		header.IsCancelled = input.Header.Game[0].IsCancelled
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": header, "function": "EventHeader", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Header Data cannot cancel"
			return nil
		}
	}

	return &games
}

func (c *DPFMAPICaller) pointTransactionCancel(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (*[]dpfm_api_output_formatter.PointTransaction) {
	sessionID := input.RuntimeSessionID
	pointTransaction := input.Header.PointTransaction[0]

	pointTransactions := make([]dpfm_api_output_formatter.PointTransaction, 0)
	for _, v := range input.Header.PointTransaction {
		data := dpfm_api_output_formatter.PointTransaction{
			Event:							input.Header.Event,
			Sender:							v.Sender,
			Receiver:						v.Receiver,
			PointConditionRecord:			v.PointConditionRecord,
			PointConditionSequentialNumber:	v.PointConditionSequentialNumber,
			IsCancelled:    				v.IsCancelled,
		}
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": data, "function": "EventPointTransaction", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Event PointTransaction Data cannot cancel"
			return nil
		}
	}

	// pointTransactionがキャンセル取り消しされた場合、headerのキャンセルも取り消す
	if !*input.Header.PointTransaction[0].IsCancelled {
		header := c.HeaderRead(input, log)
		header.IsCancelled = input.Header.PointTransaction[0].IsCancelled
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": header, "function": "EventHeader", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Header Data cannot cancel"
			return nil
		}
	}

	return &pointTransactions
}

func checkResult(msg rabbitmq.RabbitmqMessage) bool {
	data := msg.Data()
	d, ok := data["result"]
	if !ok {
		return false
	}
	result, ok := d.(string)
	if !ok {
		return false
	}
	return result == "success"
}

func getBoolPtr(b bool) *bool {
	return &b
}
