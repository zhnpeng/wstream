package items

//EncodeItem convert Item to Row then encode row
func EncodeItem(item Item) ([]byte, error) {
	var encodedBytes []byte
	row, err := item.AsRow()
	if err != nil {
		return encodedBytes, err
	}
	return row.MarshalMsg(nil)
}
