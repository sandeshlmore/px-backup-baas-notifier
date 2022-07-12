package types

type Status string

const (
	NOTFOUND  Status = "NotFound"
	AVAILABLE Status = "Available"
	PENDING   Status = "Pending"
	FAILED    Status = "Failed"
)
