/*
* Copyright 2020, Offchain Labs, Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
 */

package rollup

import (
	"context"
	"math/big"
	"time"

	"github.com/offchainlabs/arbitrum/packages/arb-util/protocol"

	"github.com/offchainlabs/arbitrum/packages/arb-util/machine"
	"github.com/offchainlabs/arbitrum/packages/arb-util/value"
	"github.com/offchainlabs/arbitrum/packages/arb-validator/structures"
)

type preparedAssertion struct {
	prevLeaf [32]byte
	params   *structures.AssertionParams
	claim    *structures.AssertionClaim
	machine  machine.Machine
}

func (chain *ChainObserver) startOpinionUpdateThread(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(time.Second)
		assertionPreparedChan := make(chan *preparedAssertion, 20)
		preparingAssertions := make(map[[32]byte]bool)
		preparedAssertions := make(map[[32]byte]*preparedAssertion)

		updateCurrent := func() {
			currentOpinion := chain.knownValidNode
			successorHashes := [4][32]byte{}
			copy(successorHashes[:], currentOpinion.successorHashes[:])
			successor := func() *Node {
				for _, successor := range currentOpinion.successorHashes {
					if successor != zeroBytes32 {
						return chain.nodeGraph.nodeFromHash[successor]
					}
				}
				return nil
			}()

			var newOpinion structures.ChildType
			prepped, found := preparedAssertions[currentOpinion.hash]

			if found &&
				prepped.params.Equals(successor.disputable.AssertionParams) &&
				prepped.claim.Equals(successor.disputable.AssertionClaim) {
				newOpinion = structures.ValidChildType
				chain.RUnlock()
			} else {
				params := successor.disputable.AssertionParams.Clone()
				claim := successor.disputable.AssertionClaim.Clone()
				claimHeight, found := chain.pendingInbox.GetHeight(claim.AfterPendingTop)
				var claimHeightCopy *big.Int
				if found {
					claimHeightCopy = new(big.Int).Set(claimHeight)
				}
				messageStack, _ := chain.pendingInbox.Substack(currentOpinion.vmProtoData.PendingTop, claim.AfterPendingTop)
				messagesVal := chain.pendingInbox.ValueForSubseq(currentOpinion.vmProtoData.PendingTop, claim.AfterPendingTop)
				prevMach := currentOpinion.machine.Clone()
				prevPendingCount := new(big.Int).Set(currentOpinion.vmProtoData.PendingCount)
				chain.RUnlock()

				newOpinion = getNodeOpinion(params, claim, prevPendingCount, claimHeightCopy, messageStack, messagesVal, prevMach)
			}
			// Reset prepared
			preparingAssertions = make(map[[32]byte]bool)
			preparedAssertions = make(map[[32]byte]*preparedAssertion)

			chain.Lock()
			correctNode, ok := chain.nodeGraph.nodeFromHash[successorHashes[newOpinion]]
			if ok {
				chain.knownValidNode = correctNode
			}
			chain.Unlock()
		}

		for {
			select {
			case <-ctx.Done():
				break
			case prepped := <-assertionPreparedChan:
				preparedAssertions[prepped.prevLeaf] = prepped
			case <-ticker.C:
				chain.RLock()
				// Catch up to current head
				for !chain.nodeGraph.leaves.IsLeaf(chain.knownValidNode) {
					updateCurrent()
					chain.RLock()
				}
				chain.RUnlock()
				// Prepare next assertion
				_, isPreparing := preparingAssertions[chain.knownValidNode.hash]
				if !isPreparing {
					preparingAssertions[chain.knownValidNode.hash] = true
					go func() {
						assertionPreparedChan <- chain.prepareAssertion(nil)
					}()
				}
			}
		}
	}()
}

func (chain *ChainObserver) prepareAssertion(
	timeBounds *protocol.TimeBoundsBlocks,
) *preparedAssertion {
	chain.RLock()
	currentOpinion := chain.knownValidNode
	currentOpinionHash := currentOpinion.hash
	if !chain.nodeGraph.leaves.IsLeaf(currentOpinion) {
		return nil
	}
	afterPendingTop := chain.pendingInbox.GetTopHash()
	beforePendingTop := currentOpinion.vmProtoData.PendingTop
	messageStack, _ := chain.pendingInbox.Substack(beforePendingTop, afterPendingTop)
	messagesVal := chain.pendingInbox.ValueForSubseq(beforePendingTop, afterPendingTop)
	mach := currentOpinion.machine.Clone()
	chain.RUnlock()

	mach.DeliverMessages(messagesVal)
	assertion, stepsRun := mach.ExecuteAssertion(chain.nodeGraph.params.MaxExecutionSteps, timeBounds)
	var params *structures.AssertionParams
	var claim *structures.AssertionClaim
	if assertion.DidInboxInsn {
		params = &structures.AssertionParams{
			NumSteps:             stepsRun,
			TimeBounds:           timeBounds,
			ImportedMessageCount: messageStack.TopCount(),
		}
		claim = &structures.AssertionClaim{
			AfterPendingTop:       afterPendingTop,
			ImportedMessagesSlice: messageStack.GetTopHash(),
			AssertionStub:         assertion.Stub(),
		}
	} else {
		params = &structures.AssertionParams{
			NumSteps:             stepsRun,
			TimeBounds:           timeBounds,
			ImportedMessageCount: big.NewInt(0),
		}
		claim = &structures.AssertionClaim{
			AfterPendingTop:       beforePendingTop,
			ImportedMessagesSlice: value.NewEmptyTuple().Hash(),
			AssertionStub:         assertion.Stub(),
		}
	}
	return &preparedAssertion{
		prevLeaf: currentOpinionHash,
		params:   params,
		claim:    claim,
		machine:  mach,
	}
}

func getNodeOpinion(
	params *structures.AssertionParams,
	claim *structures.AssertionClaim,
	prevPendingCount *big.Int,
	claimHeight *big.Int,
	messageStack *structures.MessageStack,
	messagesVal value.TupleValue,
	prevMach machine.Machine,
) structures.ChildType {
	correctAfterPendingTopHeight := new(big.Int).Add(prevPendingCount, params.ImportedMessageCount)
	if claimHeight == nil || correctAfterPendingTopHeight.Cmp(claimHeight) != 0 {
		return structures.InvalidPendingChildType
	}
	if messageStack.GetTopHash() != claim.ImportedMessagesSlice {
		return structures.InvalidMessagesChildType
	}

	mach := prevMach
	mach.DeliverMessages(messagesVal)
	assertion, stepsRun := mach.ExecuteAssertion(params.NumSteps, params.TimeBounds)
	if params.NumSteps != stepsRun || !claim.AssertionStub.Equals(assertion.Stub()) {
		return structures.InvalidExecutionChildType
	}

	return structures.ValidChildType
}
