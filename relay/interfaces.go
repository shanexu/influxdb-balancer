package relay

type Poster interface {
	Post([]byte, string, string) (*ResponseData, error)
}
