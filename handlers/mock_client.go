package handlers

type StubMistClient struct{}

func (s StubMistClient) AddStream(streamName, sourceUrl string) error {
	return nil
}

func (s StubMistClient) PushStart(streamName, targetURL string) error {
	return nil
}

func (s StubMistClient) DeleteStream(streamName string) error {
	return nil
}

func (s StubMistClient) AddTrigger(streamName, triggerName string) error {
	return nil
}

func (s StubMistClient) DeleteTrigger(streamName, triggerName string) error {
	return nil
}

func (s StubMistClient) PushAutoAdd(streamName, targetURL string, scheduleStart, scheduleEnd *int64) error {
	return nil
}
