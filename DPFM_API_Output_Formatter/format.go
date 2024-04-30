package dpfm_api_output_formatter

import (
	"database/sql"
	"fmt"
)

func ConvertToHeader(rows *sql.Rows) (*Header, error) {
	defer rows.Close()
	header := Header{}
	i := 0

	for rows.Next() {
		i++
		err := rows.Scan(
			&header.Event,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &header, err
		}

	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return nil, nil
	}

	return &header, nil
}

func ConvertToCampaign(rows *sql.Rows) (*[]Campaign, error) {
	defer rows.Close()
	campaigns := make([]Campaign, 0)
	i := 0

	for rows.Next() {
		i++
		campaign := Campaign{}
		err := rows.Scan(
			&campaign.Event,
			&campaign.Campaign,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &campaigns, err
		}

		campaigns = append(campaigns, campaign)
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &campaigns, nil
	}

	return &campaigns, nil
}

func ConvertToGame(rows *sql.Rows) (*[]Game, error) {
	defer rows.Close()
	games := make([]Game, 0)
	i := 0

	for rows.Next() {
		i++
		game := Game{}
		err := rows.Scan(
			&game.Event,
			&game.Game,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &games, err
		}

		games = append(games, game)
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &games, nil
	}

	return &games, nil
}

func ConvertToPointTransaction(rows *sql.Rows) (*[]PointTransaction, error) {
	defer rows.Close()
	pointTransactions := make([]PointTransaction, 0)
	i := 0

	for rows.Next() {
		i++
		pointTransaction := PointTransaction{}
		err := rows.Scan(
			&pointTransaction.Event,
			&pointTransaction.Sender,
			&pointTransaction.Receiver,
			&pointTransaction.PointConditionRecord,
			&pointTransaction.PointConditionSequentialNumber,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &pointTransactions, err
		}

		pointTransactions = append(pointTransactions, pointTransaction)
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &pointTransactions, nil
	}

	return &pointTransactions, nil
}
