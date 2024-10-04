[<AutoOpen>]
module GrowOnlyCounter.Transition

open System
open Microsoft.FSharp.Core
open Types
open GrowOnlyCounter

let genMessageId (node: Node) : Node * MessageId =
    {
        node with
            NextMessageId = node.NextMessageId + 1
    },
    MessageId node.NextMessageId

let withSeqKvSingleRead node (f : Node -> Value * Version -> TransitionResult) : TransitionResult =
    let node, queryMessageId = genMessageId node
    let seqKVReadMessageBody: OutputMessageBody =
        SeqKVOperation (KVRequestMessageBody.Read (queryMessageId, "sum"))
    let seqKVReadMessage =
        {
            Source = node.Info.NodeId
            Destination = NodeId.SeqKv
            MessageBody = seqKVReadMessageBody
        }

    let onSeqKvReadOk =
        fun (node: Node) (value: Value, version: Version) ->
            f node (value, version)
    let onSeqKVReadKeyDoesNotExist =
        fun (node: Node) ->
            f node (Value 0, Version 0)
    let node =
        {
            node with
                OnSeqKVReadOkHandlers = node.OnSeqKVReadOkHandlers.Add(queryMessageId, onSeqKvReadOk)
                OnSeqKVReadKeyDoesNotExistHandlers = node.OnSeqKVReadKeyDoesNotExistHandlers.Add(queryMessageId, onSeqKVReadKeyDoesNotExist)
        }
    node, [seqKVReadMessage]

let withTransformValueAndVersion node (mapper: Value * Version -> Value * Version) (f: Node -> Value * Version -> TransitionResult) : TransitionResult =

    let rec writeTransformedValue node (oldValueAndVersion) : TransitionResult =
        accumulator {
            let (node, updateMessageId: MessageId) = genMessageId node
            let newValueAndVersion = mapper oldValueAndVersion
            let updateMessageBody: OutputMessageBody =
                SeqKVOperation (KVRequestMessageBody.CompareAndSwap (updateMessageId, "sum", oldValueAndVersion,  newValueAndVersion, fst oldValueAndVersion = Value 0))
            yield
                {
                    Source = node.Info.NodeId
                    Destination = NodeId.SeqKv
                    MessageBody = updateMessageBody
                }

            let node =
                {
                    node with
                        OnSeqKVCompareAndSwapOkHandlers = node.OnSeqKVCompareAndSwapOkHandlers.Add(updateMessageId, (fun node -> f node newValueAndVersion))
                        OnSeqKVCompareAndSwapPreconditionFailedHandlers = node.OnSeqKVCompareAndSwapPreconditionFailedHandlers.Add(updateMessageId, fun node -> withSeqKvSingleRead node writeTransformedValue)
                }
            return node
        }

    withSeqKvSingleRead node writeTransformedValue

let inline transition (node: Node) (action: Choice<Message<InputMessageBody>,unit>) : TransitionResult =
    accumulator {
        match action with
        | Choice2Of2 unit ->
            return node
        | Choice1Of2 msg ->
            match msg.MessageBody with
            | Add(messageId, delta) ->
                let replyAddOkMessage =
                    {
                        Source = node.Info.NodeId
                        Destination = msg.Source
                        MessageBody = AddAck(messageId)
                    }

                let replyAddOk (node) =
                    accumulator {
                        yield replyAddOkMessage
                        return node
                    }

                if delta = Delta 0 then
                    return! replyAddOk node
                else
                    return! withTransformValueAndVersion node (fun (value, _version) -> (value + delta, Version 0)) (fun node _ -> replyAddOk node)
            | Read messageId ->
                let replyReadOk node (value, _version) =
                    let outputMessageBody: OutputMessageBody =
                        ReadAck(messageId, value)
                    let outputMessage =
                        {
                            Source = node.Info.NodeId
                            Destination = msg.Source
                            MessageBody = outputMessageBody
                        }
                    node, [outputMessage]
                return! withTransformValueAndVersion node (fun (value, version) -> (value, version + 1)) replyReadOk
            | KVResponse (KVResponseMessageBody.ReadOk(inReplyTo, value)) ->
                return!
                    node.OnSeqKVReadOkHandlers
                    |> Map.tryFind inReplyTo
                    |> Option.get
                    |> (fun f ->
                        f node value
                    )
            | KVResponse (KVResponseMessageBody.ErrorKeyDoesNotExist inReplyTo) ->
                return!
                    node.OnSeqKVReadKeyDoesNotExistHandlers
                    |> Map.tryFind inReplyTo
                    |> Option.get
                    <| node

            | KVResponse (KVResponseMessageBody.CompareAndSwapOk inReplyTo) ->
                return!
                    node.OnSeqKVCompareAndSwapOkHandlers
                    |> Map.tryFind inReplyTo
                    |> Option.get
                    <| node
            | KVResponse (KVResponseMessageBody.WriteOk inReplyTo) ->
                return!
                    failwith "WriteOk handler not implemented"
            | KVResponse (KVResponseMessageBody.ErrorPreconditionFailed inReplyTo) ->
                return!
                    node.OnSeqKVCompareAndSwapPreconditionFailedHandlers
                    |> Map.tryFind inReplyTo
                    |> Option.get
                    <| node
    }

