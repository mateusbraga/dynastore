package server

import (
	"encoding/gob"

	"github.com/mateusbraga/dynastore/pkg/view"
)

type ViewSeq []*view.View

func (viewSeq ViewSeq) Equal(otherViewSeq ViewSeq) bool {
	if len(otherViewSeq) != len(viewSeq) {
		return false
	}

	for _, loopView := range viewSeq {
		if !otherViewSeq.HasView(loopView) {
			return false
		}
	}

	return true
}

func (viewSeq ViewSeq) HasView(view *view.View) bool {
	for _, loopView := range viewSeq {
		if loopView.Equal(view) {
			return true
		}
	}
	return false
}

func (viewSeq ViewSeq) GetLeastUpdatedView() *view.View {
	leastUpdatedViewIndex := 0
	for loopIndex, loopView := range viewSeq {
		if loopView.LessUpdatedThan(viewSeq[leastUpdatedViewIndex]) {
			leastUpdatedViewIndex = loopIndex
		}
	}

	return viewSeq[leastUpdatedViewIndex]
}

func (viewSeq ViewSeq) GetMostUpdatedView() *view.View {
	mostUpdatedViewIndex := 0
	for loopIndex, loopView := range viewSeq {
		if viewSeq[mostUpdatedViewIndex].LessUpdatedThan(loopView) {
			mostUpdatedViewIndex = loopIndex
		}
	}

	return viewSeq[mostUpdatedViewIndex]
}

func (viewSeq ViewSeq) Delete(toBeDeletedView *view.View) ViewSeq {
	var index int
	for loopIndex, loopView := range viewSeq {
		if loopView.Equal(toBeDeletedView) {
			index = loopIndex
			break
		}
	}

	copy(viewSeq[index:], viewSeq[index+1:])
	return viewSeq[:len(viewSeq)-1]
}

func init() {
	gob.Register(new(ViewSeq))
}
