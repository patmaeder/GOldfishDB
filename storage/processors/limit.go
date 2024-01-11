package processors

type LimitProcessor struct {
	Limit int
}

func Limit(limit int) LimitProcessor {
	return LimitProcessor{
		Limit: limit,
	}
}
