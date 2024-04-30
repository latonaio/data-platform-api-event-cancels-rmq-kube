package dpfm_api_caller

import (
	dpfm_api_input_reader "data-platform-api-event-cancels-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-event-cancels-rmq-kube/DPFM_API_Output_Formatter"

	"fmt"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
)

func (c *DPFMAPICaller) HeaderRead(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *dpfm_api_output_formatter.Header {
	where := fmt.Sprintf("WHERE header.Event = %d ", input.Header.Event)
	rows, err := c.db.Query(
		`SELECT 
			header.Event
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_event_header_data as header ` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToHeader(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) CampaignsRead(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Campaign {
	where := fmt.Sprintf("WHERE campaign.Event IS NOT NULL\nAND header.Event = %d", input.Header.Event)
	rows, err := c.db.Query(
		`SELECT 
			campaign.Event, campaign.Campaign
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_event_campaign_data as item
		INNER JOIN DataPlatformMastersAndTransactionsMysqlKube.data_platform_event_header_data as header
		ON header.Event = campaign.Event ` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToCampaign(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) GamesRead(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Game {
	where := fmt.Sprintf("WHERE game.Event IS NOT NULL\nAND header.Event = %d", input.Header.Event)
	rows, err := c.db.Query(
		`SELECT 
			game.Event, game.Game
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_event_game_data as item
		INNER JOIN DataPlatformMastersAndTransactionsMysqlKube.data_platform_event_header_data as header
		ON header.Event = game.Event ` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToGame(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}
