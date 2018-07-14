package items

type Item interface {
	Type() ItemType
	AsRow() (Row, error)
}
