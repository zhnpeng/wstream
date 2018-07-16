package items

//EncodeItem convert Item to Row then encode row
func EncodeItem(item Item) ([]byte, error) {
	var encodedBytes []byte
	row, err := item.AsRow()
	if err != nil {
		return encodedBytes, err
	}
	return EncodeRow(row)
}

//EncodeRow encode row
func EncodeRow(row Row) ([]byte, error) {
	return row.MarshalMsg(nil)
}

//DecodeRow decode row
func DecodeRow(encodedBytes []byte) (Row, error) {
	row := Row{}
	_, err := row.UnmarshalMsg(encodedBytes)
	if err != nil {
		return row, err
	}
	return row, nil
}
