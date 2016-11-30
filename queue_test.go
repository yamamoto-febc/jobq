package jobq

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestQueue_BufSizes(t *testing.T) {

	bufSize := 3
	queue := NewQueue(bufSize)

	assert.Equal(t, cap(queue.Request), bufSize)

	assert.Equal(t, cap(queue.Logs.Error), defaultLogBufferSize)
	assert.Equal(t, cap(queue.Logs.Info), defaultLogBufferSize)
	assert.Equal(t, cap(queue.Logs.Trace), defaultLogBufferSize)
	assert.Equal(t, cap(queue.Logs.Warn), defaultLogBufferSize)

}

func TestQueue_PushRequest(t *testing.T) {
	bufSize := 10
	queue := NewQueue(bufSize)

	reqChanCheckFunc := func(c chan JobRequestAPI, f func(v JobRequestAPI)) {
		for {
			select {
			case v := <-c:
				f(v)
				return
			case <-time.After(1 * time.Second):
				assert.Fail(t, "Timeout")
				return
			}
		}
	}

	go queue.PushRequest("name", "test")
	reqChanCheckFunc(queue.Request, func(v JobRequestAPI) {
		assert.NotNil(t, v)
		assert.Equal(t, v.GetName(), "name")
		assert.Equal(t, v.GetPayload().(string), "test")
	})
}

func TestQueue_PushJobAPIQueue(t *testing.T) {

	bufSize := 10
	q := NewQueue(bufSize)
	q.CreateJobQueue("test", bufSize)

	jobChanCheckFunc := func(c chan JobAPI, f func(v JobAPI)) {
		for {
			select {
			case v := <-c:
				f(v)
				return
			case <-time.After(1 * time.Second):
				assert.Fail(t, "Timeout")
				return
			}
		}
	}

	job := NewJob("name", nil, "test")
	go q.PushJob("test", job)
	jobChanCheckFunc(q.Job["test"], func(v JobAPI) {
		assert.NotNil(t, v)
		assert.Equal(t, v.GetName(), "name")
		assert.Equal(t, v.GetPayload().(string), "test")
	})

}

func TestQueue_Stop(t *testing.T) {

	bufSize := 10
	queue := NewQueue(bufSize)

	go queue.Stop()

loop:
	for {
		select {
		case nilError := <-queue.Quit:
			assert.Nil(t, nilError)
			break loop
		case <-time.After(1 * time.Second):
			assert.Fail(t, "Timeout")
			return
		}
	}

	source := fmt.Errorf("TestQueue_Stop")
	go queue.StopByError(source)

loop2:
	for {
		select {
		case dest := <-queue.Quit:
			assert.Equal(t, source, dest)
			break loop2
		case <-time.After(1 * time.Second):
			assert.Fail(t, "Timeout")
			return
		}
	}

}

func TestQueue_PushLogs(t *testing.T) {

	bufSize := 10
	queue := NewQueue(bufSize)
	queue.CreateJobQueue("test", bufSize)

	stringChanCheckFunc := func(c chan string, f func(v string)) {
		for {
			select {
			case v := <-c:
				f(v)
				return
			case <-time.After(1 * time.Second):
				assert.Fail(t, "Timeout")
				return
			}
		}
	}

	go queue.PushInfo("info")
	stringChanCheckFunc(queue.Logs.Info, func(v string) {
		assert.NotNil(t, v)
		assert.Equal(t, v, "info")
	})

	go queue.PushTrace("trace")
	stringChanCheckFunc(queue.Logs.Trace, func(v string) {
		assert.NotNil(t, v)
		assert.Equal(t, v, "trace")
	})

	errChanCheckFunc := func(c chan error, f func(v error)) {
		for {
			select {
			case v := <-c:
				f(v)
				return
			case <-time.After(1 * time.Second):
				assert.Fail(t, "Timeout")
				return
			}
		}
	}

	go queue.PushWarn(fmt.Errorf("warn"))
	errChanCheckFunc(queue.Logs.Warn, func(v error) {
		assert.NotNil(t, v)
		assert.Equal(t, v.Error(), "warn")
	})

	go queue.PushError(fmt.Errorf("error"))
	errChanCheckFunc(queue.Logs.Error, func(v error) {
		assert.NotNil(t, v)
		assert.Equal(t, v.Error(), "error")
	})

	// エラーキュー投入時、他のキューへは影響しない
	for {
		select {
		case <-queue.Quit:
		case <-queue.Request:
		case <-queue.Job["test"]:
			assert.Fail(t, "Other queue used!!")
			return
		case <-time.After(1 * time.Second):
			// ここへくるはず
			return
		}
	}

}
