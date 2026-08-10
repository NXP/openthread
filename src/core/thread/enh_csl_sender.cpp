/*
 *  Copyright (c) 2024, The OpenThread Authors.
 *  All rights reserved.
 *
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions are met:
 *  1. Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *  2. Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *  3. Neither the name of the copyright holder nor the
 *     names of its contributors may be used to endorse or promote products
 *     derived from this software without specific prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 *  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 *  POSSIBILITY OF SUCH DAMAGE.
 */
#include "enh_csl_sender.hpp"

#if OPENTHREAD_CONFIG_ENHANCED_CSL_ENABLE
#if !OPENTHREAD_CONFIG_WAKEUP_END_DEVICE_ENABLE
#error "OPENTHREAD_CONFIG_ENHANCED_CSL_ENABLE requires OPENTHREAD_CONFIG_WAKEUP_END_DEVICE_ENABLE"
#endif

#include "instance/instance.hpp"
#include "common/log.hpp"
#include "common/time.hpp"
#include "mac/mac.hpp"
#include "thread/mesh_forwarder.hpp"
#include "thread/message_framer.hpp"
#include "thread/mle.hpp"

namespace ot {

RegisterLogModule("EnhCslSender");

EnhCslSender::EnhCslSender(Instance &aInstance)
    : InstanceLocator(aInstance)
    , mCslTxNeigh(nullptr)
    , mCslTxMessage(nullptr)
    , mFrameContext()
    , mCslTxDelay(0)
    , mCslTxBaseTime(0)
{
    InitFrameRequestAhead();
}

Neighbor *EnhCslSender::GetParent(void) const
{
    Neighbor *parent = nullptr;

    if (Get<Mle::Mle>().GetParent().IsStateValid())
    {
        parent = &Get<Mle::Mle>().GetParent();
    }
    else
    {
        parent = &Get<Mle::Mle>().GetParentCandidate();
    }
    return parent;
}

void EnhCslSender::InitFrameRequestAhead(void)
{
    static constexpr uint16_t kMaxFrameSize = 150;
    mCslFrameRequestAheadUs = Mac::kCslRequestAhead + Get<Mac::Mac>().CalculateRadioBusTransferTime(kMaxFrameSize);
}

void EnhCslSender::AddMessageForCslPeer(Message &aMessage, Neighbor &aNeighbor)
{
    // Limit queued messages to avoid buffer exhaustion
    // If we already have a message pending, don't queue more
    VerifyOrExit(aNeighbor.GetIndirectMessageCount() < 2);

    if (aNeighbor.GetIndirectMessage() == nullptr)
    {
        aNeighbor.SetIndirectMessage(&aMessage);
        aNeighbor.SetIndirectFragmentOffset(0);
    }
    aNeighbor.IncrementIndirectMessageCount();
    RescheduleCslTx();

exit:
    return;
}

void EnhCslSender::ClearAllMessagesForCslPeer(Neighbor &aNeighbor)
{
    VerifyOrExit(aNeighbor.GetIndirectMessageCount() > 0);

    for (Message &message : Get<MeshForwarder>().mSendQueue)
    {
        Get<MeshForwarder>().RemoveMessageIfNoPendingTx(message);
    }

    aNeighbor.SetIndirectMessage(nullptr);
    aNeighbor.ResetIndirectMessageCount();
    aNeighbor.ResetEnhCslTxAttempts();

    Update();

exit:
    return;
}

void EnhCslSender::Update(void)
{
    if (mCslTxMessage == nullptr)
    {
        RescheduleCslTx();
    }
    else if ((mCslTxNeigh != nullptr) && (mCslTxNeigh->GetIndirectMessage() != mCslTxMessage))
    {
        // `Mac` has already started the CSL tx, so wait for tx done callback
        // to call `RescheduleCslTx`
        mCslTxNeigh                      = nullptr;
        mFrameContext.mMessageNextOffset = 0;
    }
}

void EnhCslSender::RescheduleCslTx(void)
{
    uint32_t delay;
    uint32_t cslTxDelay;

    mCslTxNeigh = GetParent();
    VerifyOrExit(mCslTxNeigh->GetIndirectMessageCount() > 0);

    if (mCslTxNeigh->GetIndirectMessage() == nullptr)
    {
        for (Message &message : Get<MeshForwarder>().mSendQueue)
        {
            if (!message.IsDirectTransmission())
            {
                mCslTxNeigh->SetIndirectMessage(&message);
                mCslTxNeigh->SetIndirectFragmentOffset(0);
                break;
            }
        }
    }

    VerifyOrExit(mCslTxNeigh->GetIndirectMessage() != nullptr,
                 mCslTxNeigh->ResetIndirectMessageCount());

    delay = GetNextCslTransmissionDelay(*mCslTxNeigh, cslTxDelay, mCslFrameRequestAheadUs);

    mCslTxDelay = cslTxDelay;
    mCslTxBaseTime = mCslTxNeigh->GetEnhLastRxTimestamp();

    Get<Mac::Mac>().RequestEnhCslFrameTransmission(delay / 1000UL);

exit:
    return;
}

uint32_t EnhCslSender::GetNextCslTransmissionDelay(Neighbor &aNeighbor,
                                                   uint32_t &aDelayFromLastRx,
                                                   uint32_t  aAheadUs) const
{
    uint64_t radioNow      = otPlatRadioGetNow(reinterpret_cast<otInstance *>(&GetInstance()));
    uint32_t periodInUs    = aNeighbor.GetEnhCslPeriod() * kUsPerTenSymbols;
    uint64_t firstTxWindow = aNeighbor.GetEnhLastRxTimestamp() + aNeighbor.GetEnhCslPhase() * kUsPerTenSymbols;
    uint64_t nextTxWindow = radioNow - (radioNow % periodInUs) + (firstTxWindow % periodInUs);
    uint32_t nextTxDelay = 0;

    VerifyOrExit(periodInUs > 0);

    while (nextTxWindow < radioNow + aAheadUs)
    {
        nextTxWindow += periodInUs;
    }

    aDelayFromLastRx = static_cast<uint32_t>(nextTxWindow - aNeighbor.GetEnhLastRxTimestamp());
    nextTxDelay = static_cast<uint32_t>(nextTxWindow - radioNow - aAheadUs);

exit:
    return nextTxDelay;
}

uint16_t EnhCslSender::PrepareDataFrame(Mac::TxFrame &aFrame, Neighbor &aNeighbor, Message &aMessage)
{
    Ip6::Header    ip6Header;
    Mac::Addresses macAddrs;
    uint16_t       directTxOffset;
    uint16_t       nextOffset;

    // Determine the MAC source and destination addresses.

    IgnoreError(aMessage.Read(0, ip6Header));

    Get<MessageFramer>().DetermineMacSourceAddress(ip6Header.GetSource(), macAddrs);

    macAddrs.mDestination.SetExtended(aNeighbor.GetExtAddress());

    // Prepare the data frame from previous neighbor's indirect offset.
    directTxOffset = aMessage.GetOffset();
    aMessage.SetOffset(aNeighbor.GetIndirectFragmentOffset());

    nextOffset = Get<MessageFramer>().PrepareFrame(aFrame, aMessage, macAddrs);

    aMessage.SetOffset(directTxOffset);

    if (aFrame.IsCslIePresent())
    {
        uint16_t cslPeriod = aNeighbor.GetEnhCslPeriod();
        uint16_t cslPhase  = CalculateCslPhase(aNeighbor, aFrame, mCslTxDelay);

        aFrame.SetCslIe(cslPeriod, cslPhase);
    }

    return nextOffset;
}

Error EnhCslSender::PrepareFrameForNeighbor(Mac::TxFrame &aFrame, FrameContext &aContext, Neighbor &aNeighbor)
{
    Error    error   = kErrorNone;
    Message *message = aNeighbor.GetIndirectMessage();
    VerifyOrExit(message != nullptr, error = kErrorInvalidState);

    switch (message->GetType())
    {
    case Message::kTypeIp6:
        aContext.mMessageNextOffset = PrepareDataFrame(aFrame, aNeighbor, *message);

        if (message->GetSubType() == Message::kSubTypeMle && message->IsLinkSecurityEnabled())
        {
            aContext.mMessageNextOffset = message->GetLength();
            ExitNow(error = kErrorAbort);
        }
        break;

    default:
        error = kErrorNotImplemented;
        break;
    }

exit:
    return error;
}

Mac::TxFrame *EnhCslSender::HandleFrameRequest(Mac::TxFrames &aTxFrames)
{
    Mac::TxFrame *frame = nullptr;

    mCslTxNeigh = GetParent();

    VerifyOrExit(mCslTxNeigh != nullptr);
    VerifyOrExit(mCslTxNeigh->IsEnhCslSynchronized());

#if OPENTHREAD_CONFIG_MULTI_RADIO
    frame = &aTxFrames.GetTxFrame(Mac::kRadioTypeIeee802154);
#else
    frame = &aTxFrames.GetTxFrame();
#endif

    PrepareFrameForNeighbor(*frame, mFrameContext, *mCslTxNeigh);

    mCslTxMessage = mCslTxNeigh->GetIndirectMessage();
    VerifyOrExit(mCslTxMessage != nullptr, frame = nullptr);

    if (mCslTxNeigh->GetEnhCslTxAttempts() > 0)
    {
        frame->SetIsARetransmission(true);
        frame->SetSequence(mCslTxNeigh->GetIndirectDataSequenceNumber());

#if OPENTHREAD_CONFIG_MAC_CSL_RECEIVER_ENABLE
        bool isCsl = frame->IsCslIePresent();
#endif

        if (frame->GetSecurityEnabled()
#if OPENTHREAD_CONFIG_MAC_CSL_RECEIVER_ENABLE
            && !isCsl
#endif
           )
        {
            frame->SetFrameCounter(mCslTxNeigh->GetIndirectFrameCounter());
            frame->SetKeyId(mCslTxNeigh->GetIndirectKeyId());
        }
    }
    else
    {
        frame->SetIsARetransmission(false);
    }

    frame->SetTxDelay(mCslTxDelay);
    frame->SetTxDelayBaseTime(static_cast<uint32_t>(mCslTxBaseTime));

    frame->SetMaxCsmaBackoffs(0);

exit:
    return frame;
}

void EnhCslSender::HandleSentFrame(const Mac::TxFrame &aFrame, Error aError)
{
    Neighbor *neighbor = mCslTxNeigh;

    mCslTxMessage = nullptr;

    VerifyOrExit(neighbor != nullptr);

    mCslTxNeigh = nullptr;

    HandleSentFrame(aFrame, aError, *neighbor);

exit:
    return;
}

void EnhCslSender::HandleSentFrame(const Mac::TxFrame &aFrame, Error aError, Neighbor &aNeighbor)
{
    switch (aError)
    {
    case kErrorNone:
        aNeighbor.ResetEnhCslTxAttempts();
        break;

    case kErrorNoAck:
        OT_ASSERT(!aFrame.GetSecurityEnabled() || aFrame.IsHeaderUpdated());

        aNeighbor.IncrementEnhCslTxAttempts();

        if (aNeighbor.GetEnhCslTxAttempts() >= aNeighbor.GetEnhCslMaxTxAttempts())
        {
            // CSL transmission attempts reach max, consider neighbor out of sync
            aNeighbor.SetEnhCslSynchronized(false);
            aNeighbor.ResetEnhCslTxAttempts();

            if (aNeighbor.GetIndirectMessage() != nullptr &&
                aNeighbor.GetIndirectMessage()->GetType() == Message::kTypeIp6)
            {
                Get<MeshForwarder>().mCounters.mTxFailure++;
            }

            // Free ALL queued messages
            aNeighbor.SetIndirectMessage(nullptr);
            for (Message &message : Get<MeshForwarder>().mSendQueue)
            {
                if (!message.IsDirectTransmission())
                {
                    Get<MeshForwarder>().RemoveMessageIfNoPendingTx(message);
                }
            }
            aNeighbor.ResetIndirectMessageCount();
            mCslTxMessage = nullptr;
            mCslTxNeigh = nullptr;

            LogWarn("HandleSentFrame: All messages cleared due to max CSL attempts exceeded");

            // Only call BecomeDetached if we were actually a child.
            // If we're mid-attach (ChildIdRequest state), just let the Attacher timeout handle it.
            Get<Mle::Mle>().Stop();
            Get<Mle::Mle>().Start();

            ExitNow();
        }

        OT_FALL_THROUGH;

    case kErrorChannelAccessFailure:
    case kErrorAbort:

        // Even if CSL tx attempts count reaches max, the message won't be
        // dropped until indirect tx attempts count reaches max. So here it
        // would set sequence number and schedule next CSL tx.

        if (!aFrame.IsEmpty())
        {
            aNeighbor.SetIndirectDataSequenceNumber(aFrame.GetSequence());

            if (aFrame.GetSecurityEnabled() && aFrame.IsHeaderUpdated())
            {
                uint32_t frameCounter;
                uint8_t  keyId;

                IgnoreError(aFrame.GetFrameCounter(frameCounter));
                aNeighbor.SetIndirectFrameCounter(frameCounter);

                IgnoreError(aFrame.GetKeyId(keyId));
                aNeighbor.SetIndirectKeyId(keyId);
            }
        }

        if (aNeighbor.GetIndirectMessage()->GetType() == Message::kTypeIp6 &&
            aNeighbor.GetIndirectMessage()->GetSubType() == Message::kSubTypeMle &&
            aNeighbor.GetIndirectMessage()->IsLinkSecurityEnabled())
        {
            HandleSentFrameToNeighbor(aFrame, mFrameContext, aError, aNeighbor);
        }
        RescheduleCslTx();
        ExitNow();

    default:
        OT_ASSERT(false);
        OT_UNREACHABLE_CODE(break);
    }

    // Only called on the success case
    HandleSentFrameToNeighbor(aFrame, mFrameContext, kErrorNone, aNeighbor);

exit:
    return;
}

void EnhCslSender::HandleSentFrameToNeighbor(const Mac::TxFrame &aFrame,
                                             const FrameContext &aContext,
                                             otError             aError,
                                             Neighbor           &aNeighbor)
{
    Message *message    = aNeighbor.GetIndirectMessage();
    uint16_t nextOffset = aContext.mMessageNextOffset;

    if ((message != nullptr) && (nextOffset < message->GetLength()))
    {
        aNeighbor.SetIndirectFragmentOffset(nextOffset);
        RescheduleCslTx();
        ExitNow();
    }

    if (message != nullptr)
    {
        // The indirect tx of this message to the neighbor is done.

        Mac::Address macDest;

        aNeighbor.SetIndirectMessage(nullptr);
        aNeighbor.GetLinkInfo().AddMessageTxStatus(true);
        OT_ASSERT(aNeighbor.GetIndirectMessageCount() > 0);
        aNeighbor.DecrementIndirectMessageCount();

        if (!aFrame.IsEmpty())
        {
            IgnoreError(aFrame.GetDstAddr(macDest));
            Get<MeshForwarder>().LogMessage(MeshForwarder::kMessageTransmit, *message, aError, &macDest);
        }
        if (message->GetType() == Message::kTypeIp6)
        {
            (aError == kErrorNone) ? Get<MeshForwarder>().mCounters.mTxSuccess++
                                    : Get<MeshForwarder>().mCounters.mTxFailure++;
        }

        Get<MeshForwarder>().RemoveMessageIfNoPendingTx(*message);
    }

    RescheduleCslTx();

exit:
    return;
}

uint16_t EnhCslSender::CalculateCslPhase(const Neighbor &aNeighbor, const Mac::TxFrame &aFrame, uint32_t aTxDelay) const
{
    uint64_t phaseTimeUs;
    uint16_t phase = 0;
    uint64_t txTime         = otPlatRadioGetNow(reinterpret_cast<otInstance *>(&GetInstance())) + aTxDelay;
    uint64_t periodInUs     = static_cast<uint64_t>(aNeighbor.GetEnhCslPeriod()) * kUsPerTenSymbols;
    uint64_t nextWindowTime = aNeighbor.GetEnhLastRxTimestamp() + (aNeighbor.GetEnhCslPhase() * kUsPerTenSymbols);

    VerifyOrExit(periodInUs > 0);

    while (nextWindowTime < txTime)
    {
        nextWindowTime += periodInUs;
    }

    phaseTimeUs = nextWindowTime - txTime;
    phase       = static_cast<uint16_t>(phaseTimeUs / kUsPerTenSymbols);

exit:
    return phase;
}
} // namespace ot
#endif
