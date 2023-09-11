package model

type ShipmentItem struct {
	SKU    string `json:"SKU" dynamodbav:"SKU"`
	Packed bool   `json:"is_packed" dynamodbav:"is_packed"`
}

func NewShipmentItem() *ShipmentItem {
	return &ShipmentItem{}
}

func NewShipmentItemWithParams(sku string, isPacked bool) *ShipmentItem {
	return &ShipmentItem{
		SKU:    sku,
		Packed: isPacked,
	}
}
