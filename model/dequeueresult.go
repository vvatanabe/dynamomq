package model

import (
	"encoding/json"
	"github.com/vvatanabe/go82f46979/appdata"
)

type DequeueResult struct {
	*ReturnResult
	DequeuedShipmentObject *appdata.Shipment `json:"-"`
}

func NewDequeueResult() *DequeueResult {
	return &DequeueResult{}
}

func NewDequeueResultWithID(id string) *DequeueResult {
	return &DequeueResult{ReturnResult: &ReturnResult{ID: id}}
}

func FromReturnResult(result *ReturnResult) *DequeueResult {
	return &DequeueResult{
		ReturnResult: result,
	}
}

func (dr *DequeueResult) GetDequeuedShipmentObject() *appdata.Shipment {
	return dr.DequeuedShipmentObject
}

func (dr *DequeueResult) SetDequeuedShipmentObject(shipment *appdata.Shipment) {
	dr.DequeuedShipmentObject = shipment
}

func (dr *DequeueResult) GetPeekedShipmentId() string {
	if dr.DequeuedShipmentObject != nil {
		return dr.DequeuedShipmentObject.ID
	}
	return "NOT FOUND"
}

func (dr *DequeueResult) MarshalJSON() ([]byte, error) {
	type Alias DequeueResult
	return json.Marshal(&struct {
		DequeueID string `json:"dequeue_id"`
		*Alias
	}{
		DequeueID: dr.GetPeekedShipmentId(),
		Alias:     (*Alias)(dr),
	})
}
