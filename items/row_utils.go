package items

//Item2Row convert Item to Row
func Item2Row(item Item) (Row, error) {
	return item.AsRow()
}
