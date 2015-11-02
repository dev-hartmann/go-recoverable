package go_recoverable

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"time"
)

type RecoverableState int

const (
	RECOVERABLE_HEALTHY    RecoverableState = 1
	RECOVERABLE_RECOVERING RecoverableState = 2
	RECOVERABLE_FAILED     RecoverableState = 3
)

func (rs RecoverableState) String() string {
	switch rs {
	case RECOVERABLE_HEALTHY:
		return "RECOVERABLE_HEALTHY"
	case RECOVERABLE_FAILED:
		return "RECOVERABLE_FAILED"
	case RECOVERABLE_RECOVERING:
		return "RECOVERABLE_RECOVERING"
	default:
		return "UNKNOWN_RECOVERABLE_STATE"
	}
}

type Recoverable interface {
	Status() RecoverableState
	Recover() bool
	GetId() string
}

type RecoverableManager interface {
	RegisterRecoverable(recoverable Recoverable)
	DeregisterRecoverable(recoverable Recoverable)
	GetAllRecoverables() []Recoverable
	GetRecoverable(id string) Recoverable

	StartWatching()
	StopWatching()
	Status() string
}

type RecoverableUpdate struct {
	recoverableId string
	updated       int64
	removed       int64
	toState       RecoverableState
}

func NewRecoverableUpdate(recoverable Recoverable, toState RecoverableState) *RecoverableUpdate {
	return &RecoverableUpdate{
		recoverableId: recoverable.GetId(),
		toState:       toState,
		updated:       time.Now().Unix(),
	}
}

func (rcvu RecoverableUpdate) String() string {
	updatedAt := time.Unix(rcvu.updated, 0).String()
	return fmt.Sprintf("Updated recoverable:[%s] to State:[%s] at [%s]", rcvu.recoverableId, rcvu.toState, updatedAt)
}

type HealthChecker struct {
	registeredComponents map[string]Recoverable
	doneChan             chan bool
	ticker               *time.Ticker
}

func (hc *HealthChecker) RegisterRecoverable(recoverable Recoverable) {

	recovId := recoverable.GetId()
	_, ok := hc.registeredComponents[recovId]

	if !ok {
		hc.registeredComponents[recovId] = recoverable
	} else {
		log.Debug("Recoverable for Id:[%s] is already being watched!", recovId)
	}

}

func (hc *HealthChecker) DeregisterRecoverable(recoverable Recoverable) {
	recovId := recoverable.GetId()
	_, ok := hc.registeredComponents[recovId]

	if ok {
		delete(hc.registeredComponents, recovId)
	} else {
		log.Error("Recoverable for Id:[%s] could not be found!", recovId)
	}
}

func (hc HealthChecker) GetAllRecoverables() []Recoverable {

	recovSlice := make([]Recoverable, 0)

	for _, val := range hc.registeredComponents {
		recovSlice = append(recovSlice, val)
	}

	return recovSlice
}

func (hc HealthChecker) GetRecoverable(id string) Recoverable {
	val, ok := hc.registeredComponents[id]

	if ok {
		return val
	} else {
		log.Error("Recoverable for Id:[%s] could not be found!", id)
		return nil
	}
}

func (hc *HealthChecker) StartWatching() {

	if len(hc.registeredComponents) < 1 {
		log.Error("No Recoverables to watch!")
		return
	}

	hc.ticker = time.NewTicker(time.Second * 5)
	go func() {
		for {
			select {
			case <-hc.ticker.C:
				for _, recoverable := range hc.registeredComponents {
					recovering := false
					state := recoverable.Status()
					if state == RECOVERABLE_FAILED && !recovering {

						ok := recoverable.Recover()
						if !ok {
							log.Error("Could not recover")
						}
					}
				}
			case <-hc.doneChan:
				return
			}
		}
	}()
}

func (hc *HealthChecker) StopWatching() {
	hc.doneChan <- true
}

func (hc *HealthChecker) Status() string {
	return "not implemented yet"
}
