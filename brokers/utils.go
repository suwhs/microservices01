package brokers

import "log"

// helper
func NewBrokerOrFail(name string, opts map[string]any) MessageBroker {
	broker, err := NewBroker(name, opts)
	if err != nil {
		log.Fatalf("could not initialize broker '%s': %s", name, err.Error())
	}
	return broker
}
