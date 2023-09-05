package model

type ReturnResult struct {
	ID                   string           `json:"id"`
	ReturnValue          ReturnStatusEnum `json:"return_value"`
	Status               StatusEnum       `json:"status"`
	LastUpdatedTimestamp string           `json:"last_updated_timestamp"`
	Version              int              `json:"version"`
}

func NewReturnResult() *ReturnResult {
	return &ReturnResult{}
}

func NewReturnResultWithID(id string) *ReturnResult {
	return &ReturnResult{ID: id}
}

func (rr *ReturnResult) SetID(id string) {
	rr.ID = id
}

func (rr *ReturnResult) IsSuccessful() bool {
	return rr.ReturnValue == ReturnStatusEnumSUCCESS
}

func (rr *ReturnResult) GetErrorMessage() string {
	return rr.ReturnValue.GetErrorMessage()
}

func (rr *ReturnResult) GetLastUpdatedTimestamp() string {
	return rr.LastUpdatedTimestamp
}

func (rr *ReturnResult) SetLastUpdatedTimestamp(timestamp string) {
	rr.LastUpdatedTimestamp = timestamp
}

func (rr *ReturnResult) GetID() string {
	return rr.ID
}

func (rr *ReturnResult) GetReturnValue() ReturnStatusEnum {
	return rr.ReturnValue
}

func (rr *ReturnResult) SetReturnValue(value ReturnStatusEnum) {
	rr.ReturnValue = value
}

func (rr *ReturnResult) GetStatus() StatusEnum {
	return rr.Status
}

func (rr *ReturnResult) SetStatus(status StatusEnum) {
	rr.Status = status
}

func (rr *ReturnResult) GetVersion() int {
	return rr.Version
}

func (rr *ReturnResult) SetVersion(version int) {
	rr.Version = version
}
