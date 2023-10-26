package test

func NewMessageData(id string) MessageData {
	return MessageData{
		ID:    id,
		Data1: "Data 1",
		Data2: "Data 2",
		Data3: "Data 3",
		Items: []MessageItem{
			{SKU: "Item-1", Packed: true},
			{SKU: "Item-2", Packed: true},
			{SKU: "Item-3", Packed: true},
		},
	}
}

type MessageData struct {
	ID    string        `json:"id" dynamodbav:"id"`
	Items []MessageItem `json:"items" dynamodbav:"items"`
	Data1 string        `json:"data_element_1" dynamodbav:"data_1"`
	Data2 string        `json:"data_element_2" dynamodbav:"data_2"`
	Data3 string        `json:"data_element_3" dynamodbav:"data_3"`
}

type MessageItem struct {
	SKU    string `json:"SKU" dynamodbav:"SKU"`
	Packed bool   `json:"is_packed" dynamodbav:"is_packed"`
}
