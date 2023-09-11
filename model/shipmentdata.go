package model

type ShipmentData struct {
	ID    string         `json:"id" dynamodbav:"id"`
	Items []ShipmentItem `json:"items" dynamodbav:"items"`
	Data1 string         `json:"data_element_1" dynamodbav:"data_1"`
	Data2 string         `json:"data_element_2" dynamodbav:"data_2"`
	Data3 string         `json:"data_element_3" dynamodbav:"data_3"`
}

func NewShipmentData() *ShipmentData {
	return &ShipmentData{
		Items: make([]ShipmentItem, 0),
	}
}

func NewShipmentDataWithID(id string) *ShipmentData {
	return &ShipmentData{
		ID:    id,
		Items: make([]ShipmentItem, 0),
	}
}
